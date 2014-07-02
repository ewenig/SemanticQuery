package TextWise::Queue::Dispatcher;

use 5.010;
use Mouse;
use Mouse::Util::TypeConstraints;

#worker types
enum 'SocketType' => qw(UNIX TCP);

# attributes
has 'SOCK_TYPE'    => (is => 'ro', isa => 'SocketType');
has 'ZMQ_ENDPOINT' => (is => 'ro', isa => 'Str');

use strict;
use warnings;
use Carp qw(croak);
use Storable qw(freeze);
use Log::Contextual::SimpleLogger;
use Log::Contextual qw( :log ),
	-logger => Log::Contextual::SimpleLogger->new({
		levels_upto => 'debug',
		coderef => sub { print @_ },
	});

use IO::Socket::INET; # TCP/IP only for now
use ZMQ::LibZMQ3;
use ZMQ::Constants qw(ZMQ_PUB ZMQ_SNDMORE);
use TextWise::Data::URL;
use TextWise::Data::Query;

my ($context, $publisher, $socket);
my $s_interrupted = 0;
$SIG{'INT'} = \&_handler;

sub _handler {
	$s_interrupted = 1;
}

sub BUILD {
	my $self = shift;

	# ZeroMQ initialization
	$context = zmq_init();
	$publisher = zmq_socket($context, ZMQ_PUB);
	zmq_connect($publisher,$self->ZMQ_ENDPOINT);

	# Receiver socket initialization
	$socket = new IO::Socket::INET( LocalHost => '127.0.0.1', LocalPort => 5000, Proto => 'tcp', Listen => 10, ReuseAddr => 1)
}

sub loop {
	my $self = shift;

	while (!$s_interrupted) {
		my $remote = $socket->accept;
		fork and next;
		_dispatch($remote);
	}

	croak("Caught interrupt");
}

sub _dispatch {
	my $remote = shift;
	croak('Undefined socket') unless (defined($remote));
	my $task = "";

	while (<$remote>) {
		$task .= $_;
	}

	# XXX IMPLEMENT PROTOCOL BUFFERS
	# Task differentiation
	my $obj;
	if ($task =~ m/^Query/) {
		croak('Not implemented');
	} elsif ($task =~ m/^URL(.*)$/) {
		$obj = TextWise::Data::URL->new(TARGET_URL => $1);
	} else {
		croak('Task not recognized');
	}

	my $buf = freeze($obj);
	zmq_send($publisher,ref($obj),ZMQ_SNDMORE);
	zmq_send($publisher,$buf,0);

	# xxx retrieve and display results

	exit(0);
}

1;
