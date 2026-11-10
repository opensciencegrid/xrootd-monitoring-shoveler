package shoveler

import (
	"context"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// With stomp.all_brokers, the shoveler keeps one connection to every broker
// behind the stomp.url alias, instead of to the one DNS picks. Each connection
// still waits for the broker's receipt before its next message, so nothing is
// lost, but there is one message in flight per broker instead of one in total.
// This fixes the issue of the queue backing up when the brokers are far away
// from the site the collector is running and is using only one broker.
//
// The brokers behind the alias change: a broker being restarted is taken out of
// it and put back afterwards. So the alias is looked up again whenever a
// connection fails, and every brokerRefreshInterval to pick up brokers put back.
// Connections to brokers still behind the alias are kept; only those to brokers
// that left it are closed.

const brokerRefreshInterval = 10 * time.Minute // how often the alias is looked up again

// resolveBrokers returns the address of every broker behind the alias in
// hostport, as host:port, by IP family, and the alias host. A var so tests can
// point it at local brokers.
var resolveBrokers = func(hostport string) (v4 []string, v6 []string, host string, err error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil, nil, "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, nil, "", err
	}
	v4, v6 = brokerAddrs(ips, port)
	return v4, v6, host, nil
}

// reachedOverIPv6 reports whether a connection to the alias, made the way a
// single connection is, goes over IPv6: the family this host reaches the
// brokers over. A var so tests can choose.
var reachedOverIPv6 = func(hostport string) (bool, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	c, err := dialer.Dial("tcp", hostport)
	if err != nil {
		return false, err
	}
	defer func() { _ = c.Close() }()
	addr, ok := c.RemoteAddr().(*net.TCPAddr)
	return ok && addr.IP.To4() == nil, nil
}

// brokerAddrs turns the alias's addresses into host:port, sorted and by family
func brokerAddrs(ips []net.IPAddr, port string) (v4 []string, v6 []string) {
	seen := map[string]bool{}
	for _, ip := range ips {
		s := ip.IP.String()
		if seen[s] {
			continue
		}
		seen[s] = true
		if ip.IP.To4() != nil {
			v4 = append(v4, net.JoinHostPort(s, port))
		} else {
			v6 = append(v6, net.JoinHostPort(s, port))
		}
	}
	sort.Strings(v4)
	sort.Strings(v6)
	return v4, v6
}

// stompBrokers runs one stompSender per broker and keeps that set in step with
// the alias.
type stompBrokers struct {
	template StompSession // credentials, topic and alias every sender copies
	out      *stompOutput
	senders  map[string]*stompSender // by dial address
	failed   chan struct{}           // a sender's connection failed: look the alias up again
	// An alias usually lists each broker under an IPv4 and an IPv6 address, and
	// DNS does not say which pairs belong together, so the senders use one
	// family. It is chosen when the alias has both, and chosen again once every
	// sender has failed: a broker failing while others still send over the same
	// family says nothing about the family.
	familyKnown bool
	useIPv6     bool
}

func newStompBrokers(template StompSession, out *stompOutput) *stompBrokers {
	return &stompBrokers{
		template: template,
		out:      out,
		senders:  map[string]*stompSender{},
		failed:   make(chan struct{}, 1),
	}
}

// run starts the senders, and looks the alias up again when a connection fails
// and every brokerRefreshInterval, until publishing stops.
func (b *stompBrokers) run() {
	b.refresh()
	ticker := time.NewTicker(brokerRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.out.stop:
			return
		case <-ticker.C:
		case <-b.failed:
		}
		b.refresh()
	}
}

// connectionFailed asks run to look the alias up again. Failures that arrive
// before it does are folded into one lookup.
func (b *stompBrokers) connectionFailed() {
	select {
	case b.failed <- struct{}{}:
	default:
	}
}

// refresh starts a sender for every broker behind the alias that has none, and
// stops the senders of brokers no longer behind it. If the alias cannot be
// resolved, the senders already running are kept; with none running, one
// sender connects to the alias itself, as without stomp.all_brokers.
func (b *stompBrokers) refresh() {
	alias := b.template.stompUrl.String()
	v4, v6, host, err := resolveBrokers(alias)
	addrs := v4
	if len(v4) > 0 && len(v6) > 0 {
		// The family is checked the first time, and again once every sender has
		// failed, when the family itself may be what broke. Senders still on
		// their first attempt have not failed yet, so starting up costs no check.
		if !b.familyKnown || b.allFailing() {
			if useIPv6, err := reachedOverIPv6(alias); err == nil {
				b.familyKnown, b.useIPv6 = true, useIPv6
			} else {
				log.Warningln("Failed to connect to", alias, "to choose between IPv4 and IPv6, keeping the last choice:", err)
			}
		}
		if b.useIPv6 {
			addrs = v6
		}
	} else if len(v4) == 0 {
		addrs = v6
	}
	if err != nil || len(addrs) == 0 {
		if len(b.senders) > 0 {
			log.Warningln("Failed to look up the STOMP brokers behind", alias+", keeping the", len(b.senders), "connected:", err)
			return
		}
		log.Warningln("Failed to look up the STOMP brokers behind", alias+", connecting to the alias:", err)
		addrs, host = []string{""}, "" // "" dials stompUrl
	}

	want := map[string]bool{}
	for _, addr := range addrs {
		want[addr] = true
		if b.senders[addr] == nil {
			b.senders[addr] = b.startSender(addr, host)
		}
	}
	var gone []*stompSender
	for addr, s := range b.senders {
		if !want[addr] {
			log.Infoln("STOMP broker", s.session.dialAddr(), "is no longer behind", alias+", closing its connection")
			gone = append(gone, s)
			delete(b.senders, addr)
		}
	}
	// Stop them all first, then wait: each finishes its current send or connect
	// attempt, and they do it together.
	for _, s := range gone {
		s.halt()
	}
	for _, s := range gone {
		<-s.done
	}
	log.Infoln("Sending to", len(b.senders), "STOMP brokers behind", alias)
}

// allFailing reports whether every sender has failed to connect or lost its
// connection.
func (b *stompBrokers) allFailing() bool {
	if len(b.senders) == 0 {
		return false
	}
	for _, s := range b.senders {
		if !s.failing.Load() {
			return false
		}
	}
	return true
}

func (b *stompBrokers) startSender(addr string, host string) *stompSender {
	session := b.template
	session.conn = nil
	session.lost = nil
	session.addr = addr
	session.serverName = host
	s := &stompSender{
		session: &session,
		failed:  b.connectionFailed,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go func() {
		select {
		case <-b.out.stop:
			s.halt()
		case <-s.done:
		}
	}()
	go s.run(b.out)
	return s
}

// stompSender sends messages to one broker over its own connection.
type stompSender struct {
	session  *StompSession
	failed   func()        // called when the connection fails
	stop     chan struct{} // closed to stop the sender
	stopOnce sync.Once
	done     chan struct{} // closed when the sender has stopped
	// failing is set when a connect attempt fails or the connection breaks, and
	// cleared once connected.
	failing atomic.Bool
}

// run connects, then sends messages until stopped, and reconnects whenever the
// connection fails. A message that fails to send goes back to the front of the
// queue, for any sender to take, while this one reconnects: a broker that is
// down must not hold a message that another broker can send. The failure also
// has the alias looked up again, which stops this sender if its broker was
// taken out of it.
// That can reorder messages, and a message whose receipt was lost after the
// broker took it can be sent twice, as it is when publish retries.
func (s *stompSender) run(out *stompOutput) {
	defer close(s.done)
	defer s.session.disconnect()

	for {
		if err := s.session.connect(); err != nil {
			s.failing.Store(true)
			log.Errorln("Failed to connect to", s.session.dialAddr()+", retrying:", err)
		} else {
			s.failing.Store(false)
			if !s.sendUntilFailure(out) {
				return
			}
			s.failing.Store(true)
			s.failed()
		}
		// Wait before reconnecting, so a broker that takes connections but fails
		// every send is not retried in a loop; the others carry on.
		select {
		case <-s.stop:
			return
		case <-time.After(reconnectDelay):
		}
	}
}

// sendUntilFailure sends messages over the open connection. It returns false
// when the sender is stopped, and true when the connection failed.
func (s *stompSender) sendUntilFailure(out *stompOutput) bool {
	for {
		select {
		case <-s.stop:
			return false
		case <-s.session.lost:
			log.Warningln("Lost the connection to STOMP broker", s.session.dialAddr())
			return true
		case msg := <-out.messages:
			out.track(s.session, msg)
			if err := s.session.send(msg.Message); err != nil {
				log.Errorln("Failed to publish message to", s.session.dialAddr()+", requeueing it:", err)
				out.requeue(s.session, msg)
				return true
			}
			out.untrack(s.session)
			MessagesPublished.Inc()
		}
	}
}

// halt stops the sender after the message it is sending, if any. It is safe
// to call twice.
func (s *stompSender) halt() {
	s.stopOnce.Do(func() { close(s.stop) })
}
