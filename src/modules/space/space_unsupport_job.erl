%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% fixme move to datastore_models
%%% @end
%%%--------------------------------------------------------------------
-module(space_unsupport_job).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: datastore_model:key().
-type record() :: #space_unsupport_job{}.
-type doc() :: datastore_model:doc(record()).

-define(CTX, #{
    model => ?MODULE
}).

-export([save/1, save/3, get/1, get/3, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-compile({no_auto_import, [get/1]}).

%% API

save(Job) ->
    #space_unsupport_job{
        space_id = SpaceId, 
        storage_id = StorageId, 
        stage = Stage, 
        task_id = TaskId
    } = Job,
    save(gen_id(SpaceId, StorageId, Stage), Job, TaskId).

save(Key, Job, TaskId) when is_atom(Key) ->
    #space_unsupport_job{
        space_id = SpaceId, 
        storage_id = StorageId, 
        stage = Stage
    } = Job,
    save(gen_id(SpaceId, StorageId, Stage), Job, TaskId);
save(Key, Job, TaskId) ->
    {ok, _} = datastore_model:save(?CTX, #document{
        key = Key, 
        value = Job#space_unsupport_job{
            task_id = TaskId
        }
    }),
    {ok, Key}.

get(SpaceId, StorageId, Stage) ->
    get(gen_id(SpaceId, StorageId, Stage)).

-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    {ok, #document{value = Job}} = datastore_model:get(?CTX, Key),
    {ok, Job}.


-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    ok = datastore_model:delete(?CTX, Key),
    {ok, Key}.

gen_id(SpaceId, StorageId, Stage) ->
    datastore_key:new_from_digest([SpaceId, StorageId, Stage]).


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
        {slave_job_id, binary}
    ]}.
