#!/usr/bin/perl

use warnings;
use strict;
use lib 'lib/';
use TextWise::Queue::Worker;
use TextWise::Data::URL;
use Env qw(TW_API_TOKEN TARGET_URL ZMQ_ENDPOINT);
use Data::Dumper qw(Dumper);

#my $worker = TextWise::Queue::Worker->new(API_TOKEN => $TW_API_TOKEN,
#					  ZMQ_ENDPOINT => $ZMQ_ENDPOINT,
#					  MONGO_HOST => "dummy",
#					  WORKER_TYPE => "Both");

my $url = TextWise::Data::URL->new(API_TOKEN => $TW_API_TOKEN,
				   TARGET_URL => $TARGET_URL);

my $resp = $url->process;
print Dumper($resp);

1;

