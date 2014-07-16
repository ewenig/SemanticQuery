package TextWise::Logger;
require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw(log_trace log_debug log_info log_warn log_error log_fatal);

use Log::Contextual::SimpleLogger;
use Log::Contextual qw( :log ),
  -logger => Log::Contextual::SimpleLogger->new({
    levels_upto => 'debug',
    coderef => sub { print @_ },
  });

1;
