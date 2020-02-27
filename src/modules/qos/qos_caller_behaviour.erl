%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% fixme
%%% @end
%%%--------------------------------------------------------------------
% fixme name
-module(qos_caller_behaviour).
-author("Michal Stanisz").

-callback qos_traverse_finished(qos_entry:id(), traverse:id()) -> ok.

