%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the handle and handle-public model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(handle_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/oz/oz_handles.hrl").

-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
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
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"handle">>, HandleId) ->
    Auth = op_gui_utils:get_user_auth(),
    {ok, #document{
        value = #od_handle{
            handle_service = HandleServiceId,
            public_handle = PublicHandle,
            resource_id = ShareId,
            metadata = Metadata
        }}} = handle_logic:get(Auth, HandleId),
    {ok, [
        {<<"id">>, HandleId},
        {<<"handleService">>, HandleServiceId},
        {<<"share">>, ShareId},
        {<<"metadataString">>, Metadata},
        {<<"publicHandle">>, PublicHandle}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"handle">>) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"handle">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"handle">>, Data) ->
    Auth = op_gui_utils:get_user_auth(),
    HandleServiceId = proplists:get_value(<<"handleService">>, Data, <<"">>),
    Metadata = proplists:get_value(<<"metadataString">>, Data, <<"">>),
    ShareId = proplists:get_value(<<"share">>, Data, <<"">>),
    {ok, HandleId} = handle_logic:create(
        Auth, HandleServiceId, <<"Share">>, ShareId, Metadata
    ),
    {ok, ShareData} = share_data_backend:find(<<"share">>, ShareId),
    NewShareData = lists:keyreplace(
        <<"handle">>, 1, ShareData, {<<"handle">>, HandleId}
    ),
    gui_async:push_updated(<<"share">>, NewShareData),
    {ok, #handle_details{
        public_handle = PublicHandle
    }} = oz_handles:get_details(Auth, HandleId),
    {ok, [
        {<<"id">>, HandleId},
        {<<"handleService">>, HandleServiceId},
        {<<"share">>, ShareId},
        {<<"metadataString">>, Metadata},
        {<<"publicHandle">>, PublicHandle}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"handle">>, _Id, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"handle">>, _Id) ->
    gui_error:report_error(<<"Not iplemented">>).
