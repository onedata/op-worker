%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Implementation of od_error_ctx_provider_behaviour for op-worker service.
%%% @end
%%%-------------------------------------------------------------------
-module(od_error_ctx_op_provider).
-author("Bartosz Walkowicz").

-behaviour(od_error_ctx_provider_behaviour).

-include_lib("ctool/include/onedata.hrl").

%% od_error_ctx_provider_behaviour callbacks
-export([
    service/0,
    service_id/0,
    service_domain/0,
    service_release_version/0,
    service_build_version/0
]).


%%%===================================================================
%%% od_error_ctx_provider_behaviour callbacks
%%%===================================================================


-spec service() -> ?OP_WORKER.
service() -> ?OP_WORKER.


-spec service_id() -> undefined | onedata:service_id().
service_id() -> oneprovider:get_id_or_undefined().


-spec service_domain() -> undefined | binary().
service_domain() ->
    case provider_logic:get_domain() of
        {ok, Domain} -> Domain;
        _ -> undefined
    end.


-spec service_release_version() -> onedata:release_version().
service_release_version() -> op_worker:get_release_version().


-spec service_build_version() -> binary().
service_build_version() -> op_worker:get_build_version().
