%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module storing metadata used by dir_stats_collector and some collections.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector_metadata).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([update_parent/2, get_parent/1, update_dir_update_time_stats/3, get_dir_update_time_stats/1,
    delete_dir_update_time_stats/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).


-type ctx() :: datastore:ctx().


-define(CTX, #{
    model => ?MODULE
}).

% Time in seconds for document to expire after delete (one year)
% NOTE: when dir_stats_collector_metadata expires, dir_stats_collector
%       is not able to handle races between remote document delete and rename.
%       Thus, document expiry time is raised to time of synced docs' expiry.
-define(DOCUMENT_EXPIRY_TIME, 31536000).


%%%===================================================================
%%% API
%%%===================================================================

-spec update_parent(file_id:file_guid(), file_id:file_guid()) -> ok.
update_parent(Guid, Parent) ->
    Default = #dir_stats_collector_metadata{parent = Parent},

    Diff = fun(Record) ->
        {ok, Record#dir_stats_collector_metadata{parent = Parent}}
    end,

    ok = ?extract_ok(datastore_model:update(?CTX, file_id:guid_to_uuid(Guid), Diff, Default)).


-spec get_parent(file_id:file_guid()) -> file_id:file_guid() | undefined.
get_parent(Guid) ->
    case datastore_model:get(?CTX#{include_deleted => true}, file_id:guid_to_uuid(Guid)) of
        {ok, #document{value = #dir_stats_collector_metadata{parent = Parent}}} ->
            Parent;
        ?ERROR_NOT_FOUND ->
            undefined
    end.


-spec update_dir_update_time_stats(
    file_id:file_guid(),
    fun((dir_update_time_stats:stats()) -> dir_update_time_stats:stats()),
    dir_update_time_stats:stats()
) -> ok.
update_dir_update_time_stats(Guid, Diff, Default) ->
    DefaultDoc = #dir_stats_collector_metadata{dir_update_time_stats = Default},

    RecordDiff = fun
        (#dir_stats_collector_metadata{dir_update_time_stats = undefined} = Record) ->
            {ok, Record#dir_stats_collector_metadata{dir_update_time_stats = Default}};
        (#dir_stats_collector_metadata{dir_update_time_stats = Stats} = Record) ->
            {ok, Record#dir_stats_collector_metadata{dir_update_time_stats = Diff(Stats)}}
    end,

    ok = ?extract_ok(datastore_model:update(?CTX, file_id:guid_to_uuid(Guid), RecordDiff, DefaultDoc)).


-spec get_dir_update_time_stats(file_id:file_guid()) -> dir_update_time_stats:stats() | undefined.
get_dir_update_time_stats(Guid) ->
    case datastore_model:get(?CTX#{include_deleted => true}, file_id:guid_to_uuid(Guid)) of
        {ok, #document{value = #dir_stats_collector_metadata{dir_update_time_stats = Stats}}} ->
            Stats;
        ?ERROR_NOT_FOUND ->
            undefined
    end.


-spec delete_dir_update_time_stats(file_id:file_guid()) -> ok.
delete_dir_update_time_stats(Guid) ->
    Diff = fun(Record) ->
        {ok, Record#dir_stats_collector_metadata{parent = undefined}}
    end,

    case datastore_model:update(?CTX, file_id:guid_to_uuid(Guid), Diff) of
        {ok, _} -> ok;
        ?ERROR_NOT_FOUND -> ok
    end.


-spec delete(file_id:file_guid()) -> ok.
delete(Guid) ->
    ok = datastore_model:delete(?CTX#{expiry => ?DOCUMENT_EXPIRY_TIME}, file_id:guid_to_uuid(Guid)).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {parent, string},
        {dir_update_time_stats, dir_update_time_stats:get_record_struct(1)}
    ]}.