%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Description of rest handler, used by pre_handler.
%%% @end
%%%--------------------------------------------------------------------
-author("Tomasz Lichon").

-record(handler_description, {
    handler :: module(),
    handler_initial_opts :: list(),
    exception_handler :: request_exception_handler:exception_handler()
}).