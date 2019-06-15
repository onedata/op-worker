%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015-2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the data-space model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(user_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).
-export([user_record/2]).

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
find_record(<<"user">>, UserId) ->
    SessionId = op_gui_session:get_session_id(),
    case op_gui_session:get_user_id() of
        UserId ->
            {ok, user_record(SessionId, UserId)};
        _ ->
            op_gui_error:unauthorized()
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
find_all(<<"user">>) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
query(<<"user">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
query_record(<<"user">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
create_record(<<"user">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | op_gui_error:error_result().
update_record(<<"user">>, _UserId, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | op_gui_error:error_result().
delete_record(<<"user">>, _Id) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant user record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec user_record(SessionId :: session:id(), UserId :: od_user:id()) ->
    proplists:proplist().
user_record(SessionId, UserId) ->
    {ok, #document{value = #od_user{
        full_name = Name,
        default_space = DefaultSpaceValue,
        eff_spaces = EffSpaces,
        eff_handle_services = EffHServices
    }}} = user_logic:get(SessionId, UserId),
    DefaultSpace = case DefaultSpaceValue of
        undefined -> null;
        _ -> DefaultSpaceValue
    end,
    Shares = lists:foldl(
        fun(SpaceId, Acc) ->
            % Make sure that user is allowed to view shares in this space
            Authorized = space_logic:has_eff_privilege(
                SessionId, SpaceId, UserId, ?SPACE_VIEW
            ),
            case Authorized of
                true ->
                    {ok, ShareIds} = space_logic:get_shares(SessionId, SpaceId),
                    ShareIds ++ Acc;
                false ->
                    Acc
            end
        end, [], EffSpaces),
    [
        {<<"id">>, UserId},
        {<<"name">>, Name},
        {<<"defaultSpaceId">>, gs_protocol:undefined_to_null(DefaultSpace)},
        {<<"spaces">>, EffSpaces},
        {<<"shares">>, Shares},
        {<<"handleServices">>, EffHServices}
    ].
