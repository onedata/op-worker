%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for collecting information about directory 
%%% update times.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_update_time_stats).
-author("Michal Wrzeszcz").


-behavior(dir_stats_collection_behaviour).


-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([report_update_of_dir/2, report_update_of_nearest_dir/2, get_update_time/1, delete_stats/1]).

%% dir_stats_collection_behaviour callbacks
-export([acquire/1, consolidate/3, save/2, delete/1, init_dir/1, init_child/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).


-type ctx() :: datastore:ctx().


-define(CTX, #{
    model => ?MODULE
}).

-define(STAT_NAME, update_time).


%%%===================================================================
%%% API
%%%===================================================================

-spec report_update_of_dir(file_id:file_guid(), times:record() | helpers:stat() | times:time()) -> ok.
report_update_of_dir(Guid, Time) ->
    ok = dir_stats_collector:update_stats_of_dir(Guid, ?MODULE, #{?STAT_NAME => infer_update_time(Time)}).


%%--------------------------------------------------------------------
%% @doc
%% Checks file type and sets time of directory identified by Guid or time of parent if Guid represents regular
%% file or link.
%% @end
%%--------------------------------------------------------------------
-spec report_update_of_nearest_dir(file_id:file_guid(), times:record() | helpers:stat() | times:time()) -> ok.
report_update_of_nearest_dir(Guid, Time) ->
    ok = dir_stats_collector:update_stats_of_nearest_dir(Guid, ?MODULE, #{?STAT_NAME => infer_update_time(Time)}).


-spec get_update_time(file_id:file_guid()) ->
    {ok, times:time()} | ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE | ?ERROR_FORBIDDEN.
get_update_time(Guid) ->
    case dir_stats_collector:get_stats(Guid, ?MODULE, all) of
        {ok, #{?STAT_NAME := Time}} -> {ok, Time};
        Error -> Error
    end.


-spec delete_stats(file_id:file_guid()) -> ok.
delete_stats(Guid) ->
    dir_stats_collector:delete_stats(Guid, ?MODULE).


%%%===================================================================
%%% dir_stats_collection_behaviour callbacks
%%%===================================================================

-spec acquire(file_id:file_guid()) -> dir_stats_collection:collection().
acquire(Guid) ->
    case datastore_model:get(?CTX, file_id:guid_to_uuid(Guid)) of
        {ok, #document{value = #dir_update_time_stats{time = Time}}} ->
            #{?STAT_NAME => Time};
        {error, not_found} ->
            #{?STAT_NAME => 0}
    end.


-spec consolidate(dir_stats_collection:stat_name(), dir_stats_collection:stat_value(),
    dir_stats_collection:stat_value()) -> dir_stats_collection:stat_value().
consolidate(_, OldValue, NewValue) ->
    % Use max in case of provision of earlier time via dbsync
    max(OldValue, NewValue).


-spec save(file_id:file_guid(), dir_stats_collection:collection()) -> ok.
save(Guid, #{?STAT_NAME := Time}) ->
    ok = ?extract_ok(datastore_model:save(?CTX, #document{
        key = file_id:guid_to_uuid(Guid),
        value = #dir_update_time_stats{time = Time}
    })).


-spec delete(file_id:file_guid()) -> ok.
delete(Guid) ->
    ok = datastore_model:delete(?CTX, file_id:guid_to_uuid(Guid)).


-spec init_dir(file_id:file_guid()) -> dir_stats_collection:collection() | no_return().
init_dir(Guid) ->
    init(Guid).


-spec init_child(file_id:file_guid()) -> dir_stats_collection:collection() | no_return().
init_child(Guid) ->
    init(Guid).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {time, integer}
    ]}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Provides information when file or directory has been updated
%% using timestamp, times record or statbuf records
%% (update time = change of mtime or ctime)
%% @end
%%--------------------------------------------------------------------
-spec infer_update_time(times:record() | helpers:stat() | times:time()) -> times:time().
infer_update_time(#times{mtime = MTime, ctime = CTime}) ->
    max(MTime, CTime);
infer_update_time(#statbuf{st_mtime = StMtime, st_ctime = StCtime}) ->
    max(StMtime, StCtime);
infer_update_time(Timestamp) when is_integer(Timestamp) ->
    Timestamp.


%% @private
-spec init(file_id:file_guid()) -> dir_stats_collection:collection().
init(Guid) ->
    Uuid = file_id:guid_to_uuid(Guid),
    case times:get(Uuid) of
        {ok, #document{value = Times}} ->
            #{?STAT_NAME => infer_update_time(Times)};
        {error, not_found} ->
            #{?STAT_NAME => 0} % Race with file deletion - stats will be invalidated by next update
    end.