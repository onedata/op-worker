%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the share model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(share_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").


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
    {ok, proplists:proplist()} | gui_error:error_result().
find_record(ModelType, ShareId) ->
    % Make sure that user is allowed to view requested share - he must have
    % view privileges in this space, or view the share in public view.
    {Authorized, ShareRecord} = case ModelType of
        <<"share">> ->
            UserAuth = op_gui_utils:get_user_auth(),
            {ok, #document{
                value = #od_share{space = SpaceId} = ShRecord
            }} = share_logic:get(UserAuth, ShareId),
            UserId = gui_session:get_user_id(),
            HasPriv = space_logic:has_effective_privilege(
                SpaceId, UserId, ?SPACE_VIEW
            ),
            {HasPriv, ShRecord};
        <<"share-public">> ->
            {ok, #document{value = ShRecord}} = share_logic:get(provider, ShareId),
            {true, ShRecord}
    end,
    case Authorized of
        false ->
            gui_error:unauthorized();
        true ->
            share_record(ModelType, ShareId, ShareRecord)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"share-public">>) ->
    gui_error:report_error(<<"Not implemented">>);
find_all(<<"share">>) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
query(_, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
query_record(_, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(_, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"share-public">>, _ShareId, _Data) ->
    gui_error:report_error(<<"Not implemented">>);
update_record(<<"share">>, ShareId, [{<<"name">>, Name}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case Name of
        undefined ->
            ok;
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot set share name to empty string.">>);
        NewName ->
            case share_logic:set_name(UserAuth, ShareId, NewName) of
                ok ->
                    % Push container dir as its name has also changed.
                    {ok, FileData} = file_data_backend:find_record(
                        <<"file-shared">>, <<"containerDir.", ShareId/binary>>
                    ),
                    FileDataNewName = lists:keyreplace(
                        <<"name">>, 1, FileData, {<<"name">>, NewName}
                    ),
                    gui_async:push_updated(<<"file-shared">>, FileDataNewName),
                    ok;
                {error, {403, <<>>, <<>>}} ->
                    gui_error:report_warning(<<"You do not have permissions to "
                    "manage shares in this space.">>);
                _ ->
                    gui_error:report_warning(
                        <<"Cannot change share name due to unknown error.">>)
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"share-public">>, _ShareId) ->
    gui_error:report_error(<<"Not implemented">>);
delete_record(<<"share">>, ShareId) ->
    SessionId = gui_session:get_session_id(),
    case logical_file_manager:remove_share(SessionId, ShareId) of
        ok ->
            ok;
        {error, ?EACCES} ->
            gui_error:report_warning(<<"You do not have permissions to "
            "manage shares in this space.">>);
        _ ->
            gui_error:report_warning(
                <<"Cannot remove share due to unknown error.">>)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Constructs a share record for given ShareId.
%% @end
%%--------------------------------------------------------------------
-spec share_record(ModelType :: binary(), ShareId :: od_share:id(),
    ShareRecord :: od_share:info()) -> {ok, proplists:proplist()}.
share_record(ModelType, ShareId, ShareRecord) ->
    #od_share{
        name = Name,
        root_file = RootFileId,
        space = SpaceId,
        public_url = PublicURL,
        handle = Handle
    } = ShareRecord,
    HandleVal = case Handle of
        undefined -> null;
        <<"undefined">> -> null;
        _ -> Handle
    end,
    FileId = case ModelType of
        <<"share">> ->
            fslogic_uuid:share_guid_to_guid(RootFileId);
        <<"share-public">> ->
            RootFileId
    end,
    UserEntry = case ModelType of
        <<"share">> ->
            [{<<"user">>, gui_session:get_user_id()}];
        <<"share-public">> ->
            []
    end,
    {ok, [
        {<<"id">>, ShareId},
        {<<"name">>, Name},
        {<<"file">>, FileId},
        {<<"containerDir">>, <<"containerDir.", ShareId/binary>>},
        {<<"dataSpace">>, SpaceId},
        {<<"publicUrl">>, PublicURL},
        {<<"handle">>, HandleVal}
    ] ++ UserEntry}.