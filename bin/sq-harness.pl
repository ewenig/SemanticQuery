#!/usr/bin/env perl

use strict;
use warnings;
use Env qw();
use Getopt::Long qw(GetOptions);
use Pod::Usage qw(pod2usage);
use Env qw(TW_API_TOKEN ZMQ_ENDPOINT DISP_SOCK_TYPE DISP_SOCK_PORT DISP_SOCK_FILE);
use EV;

my @cpids; # child process IDs
$SIG{'INT'} = sub {
    # signal handler
    EV::unloop;
    kill 'INT', @cpids;
    die;
};

# load SemanticQuery modules
use lib 'lib';
use SemanticQuery::Queue::Dispatcher;
use SemanticQuery::Queue::Worker;
use Data::Dumper qw(Dumper);

# load num of workers to use
my $workers;
GetOptions('workers|w=i' => \$workers);

# sane defaults
$ZMQ_ENDPOINT = 'tcp://localhost:7070' unless (defined($ZMQ_ENDPOINT));
$DISP_SOCK_TYPE = 'TCP' unless (defined($DISP_SOCK_TYPE));
$DISP_SOCK_PORT = 6000 unless (defined($DISP_SOCK_PORT) || $DISP_SOCK_TYPE ne 'TCP');

# die & print usage unless things are in order
pod2usage("$0: --workers must be specified") unless (defined($workers));
pod2usage("$0: TW_API_TOKEN must be set") unless ($TW_API_TOKEN);
pod2usage("$0: DISP_SOCK_FILE must be set if socket type is UNIX") if ($DISP_SOCK_TYPE eq 'UNIX' && !defined($DISP_SOCK_FILE));

# options
my $dispatcher_opts = {
	ZMQ_PUSHPOINT => $ZMQ_ENDPOINT,
	SOCK_TYPE => $DISP_SOCK_TYPE
};
$dispatcher_opts->{'LOCAL_PORT'} = $DISP_SOCK_PORT if ($DISP_SOCK_TYPE eq 'TCP');
$dispatcher_opts->{'SOCK_FILE'} = $DISP_SOCK_FILE if ($DISP_SOCK_TYPE eq 'UNIX');

my $worker_opts = {
	API_TOKEN => $TW_API_TOKEN,
	ZMQ_PULLPOINT => $ZMQ_ENDPOINT
};

my $dispatcher = SemanticQuery::Queue::Dispatcher->new($dispatcher_opts);
my $pid = fork;
if ($pid == 0) {
    $dispatcher->loop;
    return 0;
} else {
    push @cpids, $pid;
}

for (my $ctr = 0; $ctr < $workers; $ctr++) {
	$pid = fork;
	if ($pid == 0) {
        my $worker = SemanticQuery::Queue::Worker->new($worker_opts);
	    $worker->loop;
        return 0;
    } else {
        push @cpids, $pid;
    }
}

EV::loop;

1;

__END__

=head1 NAME

sq-harness.pl - Harness for spawning SemanticQuery Dispatcher and Worker processes

=head1 SYNOPSIS

sq-harness.pl [options]

	Options:
		--workers|-w [n]		spawn [n] workers

The following environmental variables should be set before invoking sq-harness.pl.

	TW_API_TOKEN	The TextWise (http://www.textwise.com) API token to use.

The following environmental variables may be set before invoking, but are not necessary.

	ZMQ_ENDPOINT	The ZeroMQ endpoint to use (default: tcp://localhost:7070)
	DISP_SOCK_TYPE	The type of socket for Dispatcher to listen to requests on, TCP or UNIX (default: TCP)
	DISP_SOCK_PORT	The port for Dispatcher to listen on using TCP (default: 6000)
	DISP_SOCK_FILE	The named pipe for Dispatcher to listen on using UNIX sockets (default: unset)

=cut
