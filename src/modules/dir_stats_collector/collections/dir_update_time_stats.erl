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
-export([report_update_of_dir/2, report_update_of_nearest_dir/2, get_update_time/1]).

%% dir_stats_collection_behaviour callbacks
-export([
    acquire/1, consolidate/3, on_collection_move/2, save/3, delete/1, init_dir/1, init_child/2,
    compress/1, decompress/1
]).

%% datastore_model callbacks
-export([get_record_struct/1]).


-record(dir_update_time_stats, {
    time = 0 :: times:time(),
    incarnation = 0 :: non_neg_integer()
}).


-type stats() :: #dir_update_time_stats{}.
-export_type([stats/0]).


-define(STAT_NAME, update_time).


%%%===================================================================
%%% API
%%%===================================================================

-spec report_update_of_dir(file_id:file_guid(), times:record() | helpers:stat() | times:time()) -> ok.
report_update_of_dir(Guid, Time) -> % fixme do only when updating stats
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


-spec get_update_time(file_id:file_guid()) -> {ok, times:time()} | dir_stats_collector:error().
get_update_time(Guid) ->
    case dir_stats_collector:get_stats(Guid, ?MODULE, all) of
        {ok, #{?STAT_NAME := Time}} -> {ok, Time};
        Error -> Error
    end.


%%%===================================================================
%%% dir_stats_collection_behaviour callbacks
%%%===================================================================

-spec acquire(file_id:file_guid()) -> {dir_stats_collection:collection(), non_neg_integer()}.
acquire(Guid) ->
    case dir_stats_collector_metadata:get_dir_update_time_stats(Guid) of
        #dir_update_time_stats{
            time = Time,
            incarnation = Incarnation
        } ->
            {#{?STAT_NAME => Time}, Incarnation};
        undefined ->
            {#{?STAT_NAME => 0}, 0}
    end.


-spec consolidate(dir_stats_collection:stat_name(), dir_stats_collection:stat_value(),
    dir_stats_collection:stat_value()) -> dir_stats_collection:stat_value().
consolidate(_, OldValue, NewValue) ->
    % Use max in case of provision of earlier time via dbsync
    max(OldValue, NewValue).


-spec on_collection_move(dir_stats_collection:stat_name(), dir_stats_collection:stat_value()) -> ignore.
on_collection_move(_, _) ->
    ignore.


-spec save(file_id:file_guid(), dir_stats_collection:collection(), non_neg_integer() | current) -> ok.
save(Guid, #{?STAT_NAME := Time}, Incarnation) ->
    Default = #dir_update_time_stats{
        time = Time,
        incarnation = utils:ensure_defined(Incarnation, current, 0)
    },

    Diff = fun(#dir_update_time_stats{
        incarnation = CurrentIncarnation
    } = Record) ->
        NewIncarnation = utils:ensure_defined(Incarnation, current, CurrentIncarnation),
        Record#dir_update_time_stats{
            time = Time,
            incarnation = NewIncarnation
        }
    end,

    ok = ?extract_ok(dir_stats_collector_metadata:update_dir_update_time_stats(Guid, Diff, Default)).


-spec delete(file_id:file_guid()) -> ok.
delete(Guid) ->
    dir_stats_collector_metadata:delete_dir_update_time_stats(Guid).


-spec init_dir(file_id:file_guid()) -> dir_stats_collection:collection().
init_dir(Guid) ->
    init(Guid).


-spec init_child(file_id:file_guid(), boolean()) -> dir_stats_collection:collection().
init_child(Guid, _) ->
    init(Guid).


-spec compress(dir_stats_collection:collection()) -> term().
compress(#{?STAT_NAME := StatValue}) ->
    #{0 => StatValue}.


-spec decompress(term()) -> dir_stats_collection:collection().
decompress(#{0 := StatValue}) ->
    #{?STAT_NAME => StatValue}.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%% @doc
%% Definition of record struct used by datastore.
%%
%% Warning: this module is not datastore model. dir_update_time_stats
%% are stored inside dir_stats_collector_metadata datastore model. Creation of
%% dir_update_time_stats record struct's new version requires creation
%% of dir_stats_collector_metadata datastore model record struct's new version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {time, integer},
        {incarnation, integer}
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
    case fslogic_times:get(file_ctx:new_by_guid(Guid)) of
        {ok, Times} ->
            #{?STAT_NAME => infer_update_time(Times)};
        ?ERROR_NOT_FOUND ->
            #{?STAT_NAME => 0} % Race with file deletion - stats will be invalidated by next update
    end.