package SemanticQuery::Queue::Worker;

use 5.010;
use Mouse;
use Mouse::Util::TypeConstraints;

#attributes
has 'API_TOKEN'    => (is => 'ro', isa => 'Str', required => 1);
has 'MONGO_HOST'   => (is => 'ro', isa => 'Str', required => 0, default => 'localhost:27017');
has 'MONGO_USER'   => (is => 'ro', isa => 'Str', required => 0);
has 'MONGO_PASS'   => (is => 'ro', isa => 'Str', required => 0);
has 'ZMQ_PULLPOINT' => (is => 'ro', isa => 'Str', required => 1, default => 'tcp://localhost:6060');
has 'ZMQ_PUSHPOINT' => (is => 'ro', isa => 'Str', required => 1, default => 'tcp://*:7071');

use strict;
use warnings;
use Carp;
use Storable qw(freeze thaw);
use Log::Contextual::SimpleLogger;
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_PULL ZMQ_PUSH ZMQ_SNDMORE);
use AnyEvent;
use MongoDB;
use SemanticQuery::Logger;
use SemanticQuery::Data::URL;
use SemanticQuery::Data::Query;
use SemanticQuery::Data::Error;
use Data::Dumper qw(Dumper);
use constant MAX_MSGLEN => 1024;

my ($context, $subscriber, $pusher, $buf, $db);
my $s_interrupted = 0;
$SIG{'INT'} = \&_handler;

sub _handler {
	$s_interrupted = 1;
}

sub BUILD {
	my $self = shift;
	$context = zmq_init();
	$subscriber = zmq_socket($context, ZMQ_PULL);
	$pusher = zmq_socket($context, ZMQ_PUSH);
	log_debug { 'Connecting pull socket to endpoint at ' . $self->ZMQ_PULLPOINT };
	my $res = zmq_connect($subscriber,$self->ZMQ_PULLPOINT);
	die("Couldn't connect to ZMQ endpoint " . $self->ZMQ_PULLPOINT . ", got error " . zmq_strerror(zmq_errno)) unless ($res == 0);
	log_debug { 'Binding push socket to endpoint at ' . $self->ZMQ_PUSHPOINT };
	$res = zmq_bind($pusher,$self->ZMQ_PUSHPOINT);
	die("Couldn't bind to ZMQ endpoint " . $self->ZMQ_PUSHPOINT . ", got error " . zmq_strerror(zmq_errno)) unless ($res == 0);

	#filter out messages using type hints
	#zmq_setsockopt($subscriber, ZMQ_SUBSCRIBE, 'SemanticQuery::Data::Query') if ($self->WORKER_TYPE eq 'Query' || $self->WORKER_TYPE eq 'Both');
	#zmq_setsockopt($subscriber, ZMQ_SUBSCRIBE, 'SemanticQuery::Data::URL') if ($self->WORKER_TYPE eq 'URL' || $self->WORKER_TYPE eq 'Both');

	# MongoDB initialization
	my $mongo = new MongoDB::MongoClient(host => $self->MONGO_HOST);
	$db = $mongo->get_database('SemanticQuery');
	log_debug { 'Established MongoDB connection at ' . $self->MONGO_HOST };
}

sub loop {
	my $self = shift;
	log_info { 'Beginning main Worker loop' };

	while (!$s_interrupted) {
		my ($envelope,$req_id,$obj_blob,$obj) = ('','','');
		log_debug { 'Receiving message envelope' };
		$envelope = s_recv($subscriber) while ($envelope eq '');
		log_debug { "Got envelope with type hint " . Dumper($envelope) };
		$req_id = s_recv($subscriber) while ($req_id eq '');
		log_debug { "Got request id $req_id" }
		$obj_blob = s_recv($subscriber) while ($obj_blob eq '');

		# error handling
		local $SIG{__WARN__} = sub {
			my $obj = new SemanticQuery::Data::Error(ERR_MESSAGE => shift);
			my $obj_blob = freeze($obj);
			zmq_send($pusher,$req_id,ZMQ_SNDMORE);
			zmq_send($pusher,$obj_blob,0);
		};

		$obj = thaw($obj_blob);
		log_debug { 'Got Data object with type ' . ref($obj) }
		$obj->set_api_token($self->API_TOKEN);
		$obj->set_mongo_ptr($db);
		my $resp = _process_msg($obj);
		my $resp_blob = freeze($resp);
		zmq_send($pusher,$req_id,ZMQ_SNDMORE);
		zmq_send($pusher,$resp_blob,0);
	}

	# interrupt caught
	log_warn { 'Caught interrupt' };
	croak("Caught interrupt");
}

sub _process_msg {
	my $object = shift;
	my $result = $object->process;
	log_debug { Dumper($result) };
	return $result;
}

sub s_recv {
	my $sock = shift;
	my $buf;
	my $size = zmq_recv($sock, $buf, MAX_MSGLEN);
	return undef if ($size > 0 || !(defined($buf)));
	return substr($buf, 0, $size);
}

1;

=pod

=head1 SemanticQuery::Queue::Worker

Worker listens on a ZeroMQ socket for requests over the wire or IPC socket.
Requests come in as serialized Perl objects. The Worker object unserializes them
and processes them (using the native processing functions included in the data
object), returning the processed data to the Dispatcher function.

Worker presently handles these types of requests:

=over

=item SemanticQuery::Data::URL - URL input requests
=item SemanticQuery::Data::Query - Request to query the database
=item SemanticQuery::Data::Error - Called in case of error

=back

=cut
