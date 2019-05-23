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

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

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
find_record(ModelType, HandleId) ->
    case ModelType of
        <<"handle">> ->
            SessionId = op_gui_session:get_session_id(),
            handle_record(SessionId, HandleId);
        <<"handle-public">> ->
            public_handle_record(HandleId)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
find_all(_) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
query(_, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
query_record(_, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
create_record(<<"handle-public">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>);
create_record(<<"handle">>, Data) ->
    SessionId = op_gui_session:get_session_id(),
    HandleServiceId = proplists:get_value(<<"handleService">>, Data, <<"">>),
    Metadata = proplists:get_value(<<"metadataString">>, Data, <<"">>),
    ShareId = proplists:get_value(<<"share">>, Data, <<"">>),
    {ok, HandleId} = handle_logic:create(
        SessionId, HandleServiceId, <<"Share">>, ShareId, Metadata
    ),
    {ok, ShareData} = share_data_backend:find_record(<<"share">>, ShareId),
    NewShareData = lists:keyreplace(
        <<"handle">>, 1, ShareData, {<<"handle">>, HandleId}
    ),
    op_gui_async:push_updated(<<"share">>, NewShareData),
    {ok, #document{value = #od_handle{
        public_handle = PublicHandle
    }}} = handle_logic:get(SessionId, HandleId),
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
    ok | op_gui_error:error_result().
update_record(_, _Id, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | op_gui_error:error_result().
delete_record(_, _Id) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a handle record for given HandleId, depending on given SessionId.
%% @end
%%--------------------------------------------------------------------
-spec handle_record(SessionId :: session:id(),
    HandleId :: binary()) -> {ok, proplists:proplist()}.
handle_record(SessionId, HandleId) ->
    case handle_logic:get(SessionId, HandleId) of
        {error, _} ->
            op_gui_error:unauthorized();
        {ok, #document{value = HandleRecord}} ->
            #od_handle{
                handle_service = HandleServiceId,
                public_handle = PublicHandle,
                resource_id = ShareId,
                metadata = Metadata
            } = HandleRecord,
            {ok, [
                {<<"id">>, HandleId},
                {<<"handleService">>, HandleServiceId},
                {<<"share">>, ShareId},
                {<<"metadataString">>, Metadata},
                {<<"publicHandle">>, PublicHandle}
            ]}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a public handle record for given HandleId.
%% @end
%%--------------------------------------------------------------------
-spec public_handle_record(HandleId :: binary()) -> {ok, proplists:proplist()}.
public_handle_record(HandleId) ->
    case handle_logic:get_public_data(?GUEST_SESS_ID, HandleId) of
        {error, _} ->
            op_gui_error:unauthorized();
        {ok, #document{value = HandleRecord}} ->
            #od_handle{
                public_handle = PublicHandle,
                metadata = Metadata
            } = HandleRecord,
            {ok, [
                {<<"id">>, HandleId},
                {<<"metadataString">>, Metadata},
                {<<"publicHandle">>, PublicHandle}
            ]}
    end.