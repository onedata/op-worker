%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The protocol between worker_host and the rest of the world
%%% @end
%%%-------------------------------------------------------------------
-author("Tomasz Lichon").

-record(worker_request, {id = undefined, req = undefined, reply_to = undefined}).
-record(worker_answer, {id = undefined, response = undefined}).
