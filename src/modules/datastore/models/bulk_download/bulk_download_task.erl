%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for persisting information required during bulk download.
%%% @end
%%%--------------------------------------------------------------------
-module(bulk_download_task).
-author("Michal Stanisz").


-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([save_main_pid/2, get_main_pid/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-record(bulk_download_task, {
    pid :: pid()
}).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all,
    disc_driver => undefined
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec save_main_pid(bulk_download:id(), pid()) -> ok.
save_main_pid(BulkDownloadId, Pid) ->
    ?extract_ok(datastore_model:save(?CTX, 
        #document{key = BulkDownloadId, value = #bulk_download_task{pid = Pid}})).


-spec get_main_pid(bulk_download:id()) -> {ok, pid()} | {error, term()}.
get_main_pid(BulkDownloadId) ->
    case datastore_model:get(?CTX, BulkDownloadId) of
        {ok, #document{value = #bulk_download_task{pid = Pid}}} -> 
            case is_process_alive(Pid) of
                true -> {ok, Pid};
                false -> 
                    delete(BulkDownloadId),
                    ?ERROR_NOT_FOUND
            end;
        {error, _} = Error -> Error
    end.


-spec delete(bulk_download:id()) -> ok | {error, term()}.
delete(BulkDownloadId) ->
    ?ok_if_not_found(datastore_model:delete(?CTX, BulkDownloadId)).


%%%===================================================================
%%% datastore model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.
