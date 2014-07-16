package TextWise::Data::Error;

use 5.010;
use Mouse;

# attributes
has 'ERR_MESSAGE' => (is => 'ro', isa => 'Int', required => 1);
has 'ERR_CODE'    => (is => 'ro', isa => 'Int', required => 0, default => 100);

1;
