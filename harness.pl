#!/usr/bin/perl

use strict;
use warnings;
use Env qw();
use Getopt::Long qw(GetOptions);
use Pod::Usage qw(pod2usage);
use Env qw(TW_API_TOKEN ZMQ_ENDPOINT DISP_SOCK_TYPE DISP_SOCK_PORT DISP_SOCK_FILE);

use Enbugger::OnError 'USR1';

# load SemanticQuery modules
use lib 'lib';
use SemanticQuery::Queue::Dispatcher;
use SemanticQuery::Queue::Worker;

# load num of workers to use
my $workers;
GetOptions('workers|w' => \$workers);

# sane defaults
$ZMQ_ENDPOINT = 'tcp://localhost:7070' unless (defined($ZMQ_ENDPOINT));
$DISP_SOCK_TYPE = 'TCP' unless (defined($DISP_SOCK_TYPE));
$DISP_SOCK_PORT = 5000 unless (defined($DISP_SOCK_PORT) || $DISP_SOCK_TYPE ne 'TCP');

# die & print usage unless things are in order
pod2usage("$0: --workers must be specified") unless (defined($workers));
pod2usage("$0: TW_API_TOKEN must be set") unless ($TW_API_TOKEN);
pod2usage("$0: DISP_SOCK_FILE must be set if socket type is UNIX") if ($DISP_SOCK_TYPE eq 'UNIX' && !defined($DISP_SOCK_FILE));

# options
my $dispatcher_opts = {
	ZMQ_ENDPOINT => $ZMQ_ENDPOINT,
	SOCK_TYPE => $DISP_SOCK_TYPE
};
$dispatcher_opts->{'LOCAL_PORT'} = $DISP_SOCK_PORT if ($DISP_SOCK_TYPE eq 'TCP');
$dispatcher_opts->{'SOCK_FILE'} = $DISP_SOCK_FILE if ($DISP_SOCK_TYPE eq 'UNIX');

my $worker_opts = {
	API_TOKEN => $TW_API_TOKEN,
	ZMQ_ENDPOINT => $ZMQ_ENDPOINT
};

my $dispatcher = SemanticQuery::Queue::Dispatcher->new($dispatcher_opts);
fork and $dispatcher->loop;

for (my $ctr = 0; $ctr < $workers; $ctr++) {
	fork and next;
	my $worker = SemanticQuery::Queue::Worker->new($worker_opts);
	$worker->loop;
}

1;

__END__

=head1 NAME

harness.pl - Harness for spawning SemanticQuery Dispatcher and Worker processes

=head1 SYNOPSIS

harness.pl [options]

	Options:
		--workers|-w [n]		spawn [n] workers

The following environmental variables should be set before invoking harness.pl.

	TW_API_TOKEN	The TextWise (http://www.textwise.com) API token to use.

The following environmental variables may be set before invoking, but are not necessary.

	ZMQ_ENDPOINT	The ZeroMQ endpoint to use (default: tcp://localhost:7070)
	DISP_SOCK_TYPE	The type of socket for Dispatcher to listen to requests on, TCP or UNIX (default: TCP)
	DISP_SOCK_PORT	The port for Dispatcher to listen on using TCP (default: 5000)
	DISP_SOCK_FILE	The named pipe for Dispatcher to listen on using UNIX sockets (default: unset)

=cut
