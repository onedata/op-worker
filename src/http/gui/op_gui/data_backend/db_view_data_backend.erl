%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% db-view model for ember app.
%%% @end
%%%-------------------------------------------------------------------
-module(db_view_data_backend).
-behavior(data_backend_behaviour).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").


%% API
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).

%%%===================================================================
%%% data_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
find_record(<<"db-view">>, RecordId) ->
    SessionId = op_gui_session:get_session_id(),
    {ok, UserId} = session:get_user_id(SessionId),

    {ok, ViewPropList} = Result = db_view_record(RecordId),
    SpaceId = proplists:get_value(<<"space">>, ViewPropList),

    case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_VIEWS) of
        true ->
            Result;
        false ->
            op_gui_error:unauthorized()
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
find_all(_ResourceType) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
query(_ResourceType, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
query_record(_ResourceType, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
create_record(_ResourceType, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | op_gui_error:error_result().
update_record(_ResourceType, _Id, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | op_gui_error:error_result().
delete_record(_ResourceType, _Id) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant db view record based on space id and view name.
%% @end
%%--------------------------------------------------------------------
-spec db_view_record(ViewId :: binary()) -> {ok, proplists:proplist()}.
db_view_record(ViewId) ->
    {ok, #document{value = #index{
        name = ViewName,
        space_id = SpaceId,
        index_options = ViewOptions,
        providers = Providers,
        map_function = MapFunction,
        reduce_function = ReduceFunction,
        spatial = Spatial
    }}} = index:get(ViewId),

    {ok, [
        {<<"id">>, ViewId},
        {<<"name">>, ViewName},
        {<<"space">>, SpaceId},
        {<<"viewOptions">>, {ViewOptions}},
        {<<"providers">>, Providers},
        {<<"mapFunction">>, MapFunction},
        {<<"reduceFunction">>, utils:ensure_defined(ReduceFunction, undefined, null)},
        {<<"spatial">>, Spatial}
    ]}.
