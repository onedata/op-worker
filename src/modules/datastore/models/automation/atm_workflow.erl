%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about workflow.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).


-type id() :: binary().
-type diff() :: datastore_doc:diff(record()).
-type record() :: #atm_workflow{}.
-type doc() :: datastore_doc:doc(record()).
-type error() :: {error, term()}.

-export_type([id/0, record/0, doc/0]).


-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create(file_meta:uuid(), od_space:id()) -> ok | error().
create(WorkflowId, SpaceId) ->
  ?extract_ok(datastore_model:create(?CTX, #document{
      key = WorkflowId,
      value = #atm_workflow{
          space_id = SpaceId,
          schedule_time = global_clock:timestamp_seconds()
      }
  })).


-spec delete(id()) -> ok | error().
delete(WorkflowId) ->
    datastore_model:delete(?CTX, WorkflowId).


%%%===================================================================
%%% Datastore callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {space_id, string},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer}
    ]}.
