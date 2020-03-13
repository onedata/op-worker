%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This model holds information about current space unsupport job, i.e. what space is being 
%%% unsuported, from which storage and what stage is currently running.
%%% Some stages start internal task (like starting cleanup traverse or adding QoS entry). 
%%% This tasks should not be started again after provider was restarted. For this purpose 
%%% id of such internal task is persisted in this model as substask_id.
%%% @end
%%%--------------------------------------------------------------------
-module(space_unsupport_job).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: datastore_model:key().
-type record() :: #space_unsupport_job{}.

-export_type([record/0]).

-export([save/1, save/3, get/1, get/3, delete/1, delete/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).
-export([encode_subtask_id/1, decode_subtask_id/1]).
-export([encode_slave_job_pid/1, decode_slave_job_pid/1]).

-compile({no_auto_import, [get/1]}).


-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec save(record()) -> {ok, id()} | {error, term()}.
save(Job) ->
    #space_unsupport_job{
        space_id = SpaceId, 
        storage_id = StorageId, 
        stage = Stage, 
        task_id = TaskId
    } = Job,
    save(gen_id(SpaceId, StorageId, Stage), Job, TaskId).

-spec save(id() | undefined | main_job, record(), traverse:id()) -> 
    {ok, id()} | {error, term()}.
save(main_job, Job, TaskId) ->
    save(undefined, Job, TaskId);
save(undefined, Job, TaskId) ->
    #space_unsupport_job{
        space_id = SpaceId, 
        storage_id = StorageId, 
        stage = Stage
    } = Job,
    save(gen_id(SpaceId, StorageId, Stage), Job, TaskId);
save(Key, Job, TaskId) ->
    ?extract_key(datastore_model:save(?CTX, #document{
        key = Key, 
        value = Job#space_unsupport_job{
            task_id = TaskId
        }
    })).


-spec get(od_space:id(), storage:id(), space_unsupport:stage()) -> 
    {ok, record()} | {error, term()}.
get(SpaceId, StorageId, Stage) ->
    get(gen_id(SpaceId, StorageId, Stage)).

-spec get(id()) -> {ok, record()} | {error, term()}.
get(Key) ->
    {ok, #document{value = Job}} = datastore_model:get(?CTX, Key),
    {ok, Job}.


-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

-spec delete(od_space:id(), storage:id(), space_unsupport:stage()) -> ok | {error, term()}.
delete(SpaceId, StorageId, Stage) ->
    delete(gen_id(SpaceId, StorageId, Stage)).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {stage, atom},
        {task_id, string},
        {space_id, string},
        {storage_id, string},
        {substask_id, {custom, string, {?MODULE, encode_subtask_id, decode_subtask_id}}},
        {slave_job_pid, {custom, string, {?MODULE, encode_slave_job_pid, decode_slave_job_pid}}}
    ]}.


-spec encode_subtask_id(space_unsupport:subtask_id() | undefined) -> binary().
encode_subtask_id(undefined) -> <<"undefined">>;
encode_subtask_id(Binary) when is_binary(Binary) -> Binary.


-spec decode_subtask_id(binary()) -> space_unsupport:subtask_id() | undefined.
decode_subtask_id(<<"undefined">>) -> undefined;
decode_subtask_id(Binary) when is_binary(Binary) -> Binary.


-spec encode_slave_job_pid(pid() | undefined) -> binary().
encode_slave_job_pid(undefined) -> <<"undefined">>;
encode_slave_job_pid(Pid) -> list_to_binary(pid_to_list(Pid)).


-spec decode_slave_job_pid(binary()) -> pid() | undefined.
decode_slave_job_pid(<<"undefined">>) -> undefined;
decode_slave_job_pid(Pid) -> list_to_pid(binary_to_list(Pid)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec gen_id(od_space:id(), storage:id(), space_unsupport:stage()) -> id().
gen_id(SpaceId, StorageId, Stage) ->
    datastore_key:adjacent_from_digest([StorageId, Stage], SpaceId).
