// package Graphite provides a tcp and/or udp listener that you can
// use to ingest metrics into influxdb via the graphite protocol.  it
// behaves as a carbon daemon, except:

// no rounding of timestamps to the nearest interval.  Upon ingestion
// of multiple datapoints for a given key within the same interval
// (possibly but not necessarily the same timestamp), graphite would
// use one (the latest received) value with a rounded timestamp
// representing that interval.  We store values for every timestamp we
// receive (only the latest value for a given metric-timestamp pair)
// so it's up to the user to feed the data in proper intervals (and
// use round intervals if you plan to rely on that)
package graphite

import (
	"bufio"
	"io"
	"net"
	"strings"
	"time"

	"github.com/influxdb/influxdb/cluster"
	. "github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/coordinator"
	"github.com/influxdb/influxdb/protocol"

	log "code.google.com/p/log4go"
)

type Server struct {
	listenAddress string
	database      string
	coordinator   coordinator.Coordinator
	clusterConfig *cluster.ClusterConfiguration
	conn          net.Listener
	udpConn       *net.UDPConn
	user          *cluster.ClusterAdmin
	shutdown      chan bool
	udpEnabled    bool
}

// TODO: check that database exists and create it if not
func NewServer(config *configuration.Configuration, coord coordinator.Coordinator, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}
	self.listenAddress = config.GraphitePortString()
	self.database = config.GraphiteDatabase
	self.coordinator = coord
	self.shutdown = make(chan bool, 1)
	self.clusterConfig = clusterConfig
	self.udpEnabled = config.GraphiteUdpEnabled

	return self
}

// getAuth assures that the user property is a user with access to the graphite database
// only call this function after everything (i.e. Raft) is initialized, so that there's at least 1 admin user
func (self *Server) getAuth() {
	// just use any (the first) of the list of admins.
	names := self.clusterConfig.GetClusterAdmins()
	self.user = self.clusterConfig.GetClusterAdmin(names[0])
}

func (self *Server) ListenAndServe() {
	self.getAuth()
	var err error
	if self.listenAddress != "" {
		self.conn, err = net.Listen("tcp", self.listenAddress)
		if err != nil {
			log.Error("GraphiteServer: Listen: ", err)
			return
		}
	}
	if self.udpEnabled {
		udpAddress, _ := net.ResolveUDPAddr("udp", self.listenAddress)
		self.udpConn, _ = net.ListenUDP("udp", udpAddress)
		go self.ServeUdp(self.udpConn)
	}
	self.Serve(self.conn)
}

func (self *Server) Serve(listener net.Listener) {
	// not really sure of the use of this shutdown channel,
	// as all handling is done through goroutines. maybe we should use a waitgroup
	defer func() { self.shutdown <- true }()

	for {
		conn_in, err := listener.Accept()
		if err != nil {
			log.Error("GraphiteServer: Accept: ", err)
			continue
		}
		go self.handleClient(conn_in)
	}
}

func (self *Server) ServeUdp(conn *net.UDPConn) {
	var buf []byte = make([]byte, 65536)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Warn("Error when reading from UDP connection %s", err.Error())
		}
		go self.handleUdpMessage(string(buf[:n]))
	}
}

func (self *Server) handleUdpMessage(msg string) {
	metrics := strings.Split(msg, "\n")
	for _, metric := range metrics {
		reader := bufio.NewReader(strings.NewReader(metric + "\n"))
		go self.handleMessage(reader)
	}
}

func (self *Server) Close() {
	if self.udpConn != nil {
		log.Info("GraphiteService: Closing graphite UDP listener")
		self.udpConn.Close()
	}
	if self.conn != nil {
		log.Info("GraphiteServer: Closing graphite server")
		self.conn.Close()
		log.Info("GraphiteServer: Waiting for all graphite requests to finish before killing the process")
		select {
		case <-time.After(time.Second * 5):
			log.Error("GraphiteServer: There seems to be a hanging graphite request. Closing anyway")
		case <-self.shutdown:
		}
	}
}

func (self *Server) writePoints(series *protocol.Series) error {
	serie := []*protocol.Series{series}
	err := self.coordinator.WriteSeriesData(self.user, self.database, serie)
	if err != nil {
		switch err.(type) {
		case AuthorizationError:
			// user information got stale, get a fresh one (this should happen rarely)
			self.getAuth()
			err = self.coordinator.WriteSeriesData(self.user, self.database, serie)
			if err != nil {
				log.Warn("GraphiteServer: failed to write series after getting new auth: %s", err.Error())
			}
		default:
			log.Warn("GraphiteServer: failed write series: %s", err.Error())
		}
	}
	return err
}

func (self *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		err := self.handleMessage(reader)
		if err != nil {
			if io.EOF == err {
				log.Debug("Client closed graphite connection")
				return
			}
			log.Error(err)
			return
		}
	}
}

func (self *Server) handleMessage(reader *bufio.Reader) error {
	graphiteMetric := &GraphiteMetric{}
	err := graphiteMetric.Read(reader)
	if err != nil {
		return err
	}
	values := []*protocol.FieldValue{}
	if graphiteMetric.isInt {
		values = append(values, &protocol.FieldValue{Int64Value: &graphiteMetric.integerValue})
	} else {
		values = append(values, &protocol.FieldValue{DoubleValue: &graphiteMetric.floatValue})
	}
	sn := uint64(1) // use same SN makes sure that we'll only keep the latest value for a given metric_id-timestamp pair
	point := &protocol.Point{
		Timestamp:      &graphiteMetric.timestamp,
		Values:         values,
		SequenceNumber: &sn,
	}
	series := &protocol.Series{
		Name:   &graphiteMetric.name,
		Fields: []string{"value"},
		Points: []*protocol.Point{point},
	}
	// little inefficient for now, later we might want to add multiple series in 1 writePoints request
	if err := self.writePoints(series); err != nil {
		log.Error("Error in graphite plugin: %s", err)
	}
	return nil
}
