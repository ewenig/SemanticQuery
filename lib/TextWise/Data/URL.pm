package TextWise::Data::URL;

use Carp;
use Mouse;

# attributes
has 'API_TOKEN'  => (is => 'ro', isa => 'Str', required => 0);
has 'TARGET_URL' => (is => 'ro', isa => 'Str', required => 1);
has 'DB_HOST'    => (is => 'ro', isa => 'Str', required => 0, default => 'localhost:27017');

use strict;
use warnings;
use TextWise::Logger;

sub process {
	# lazy package loading
	require TextWise::API::Category;
	require TextWise::API::Concept;
	require MongoDB;

	my $self = shift;
	unless (defined($self->API_TOKEN)) {
		carp(__PACKAGE__ . ' requires API token');
		return;
	}

	my $categorizer = TextWise::API::Category->new(API_TOKEN => $self->API_TOKEN);
	my $conceiver = TextWise::API::Concept->new(API_TOKEN => $self->API_TOKEN);

	my $categories = $categorizer->process($self->TARGET_URL);
	my $concepts = $conceiver->process($self->TARGET_URL);

	my $db_client = new MongoDB::MongoClient(host => $self->DB_HOST);
	my $db = $db_client->get_database("SemanticQuery");
	my $doc_id = $db->get_collection('entries')->insert({
		url => $self->TARGET_URL,
		categories => $categories,
		concepts => $concepts
	});

	my $ret = {
		status => "OK",
		id => $doc_id->to_string
	};
	return $ret;
}

1;
