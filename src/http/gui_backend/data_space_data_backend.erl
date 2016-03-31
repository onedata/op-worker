%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Jakub Liput
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
-module(data_space_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").


% @todo currently unused - every time taken from OZ
%% Key under which default space is stored in session memory.
-define(DEFAULT_SPACE_KEY, default_space).

%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).


% @todo dev
-export([something_changed/0]).

%%--------------------------------------------------------------------
%% @doc
%% @todo temporal solution - until events are used in GUI
%% Lists all spaces again and pushes the data to the client to impose refresh.
%% @end
%%--------------------------------------------------------------------
-spec something_changed() -> ok.
something_changed() ->
    lists:foreach(
        fun({SessionId, Pids}) ->
            try
                g_session:set_session_id(SessionId),
                {ok, Data} = find_all(<<"data-space">>),
                DefSpace = op_gui_utils:get_users_default_space(),
                lists:map(
                    fun(Props) ->
                        case proplists:get_value(<<"id">>, Props) of
                            DefSpace ->
                                [{<<"isDefault">>, true} |
                                    proplists:delete(<<"isDefault">>, Props)];
                            _ ->
                                Props
                        end
                    end, Data),
                lists:foreach(
                    fun(Pid) ->
                        gui_async:push_updated(<<"data-space">>, Data, Pid)
                    end, Pids)
            catch T:M ->
                ?dump({T, M, erlang:get_stacktrace()})
            end
        end, op_gui_utils:get_all_backend_pids(?MODULE)),
    ok.


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    % Resolve default space and put it in session memory
    % NOTE that SpaceDir UUID is remembered rather than Space ID
%%    DefaultSpaceId = op_gui_utils:get_users_default_space(),
%%    DefaultSpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(DefaultSpaceId),
%%    g_session:put_value(?DEFAULT_SPACE_KEY, DefaultSpaceDirId),
    op_gui_utils:register_backend(?MODULE, self()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    op_gui_utils:unregister_backend(?MODULE, self()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Ids :: [binary()]) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"data-space">>, [SpaceDirId]) ->
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    {ok, #document{
        value = #space_info{
            name = Name
        }}} = space_info:get(SpaceId),
    DefaultSpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(
        op_gui_utils:get_users_default_space()),
    Res = [
        {<<"id">>, SpaceDirId},
        {<<"name">>, Name},
        {<<"isDefault">>, SpaceDirId =:= DefaultSpaceDirId},
        {<<"rootDir">>, SpaceDirId}
    ],
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_all(<<"data-space">>) ->
    UserId = op_gui_utils:get_user_id(),
    {ok, SpaceIds} = onedata_user:get_spaces(UserId),
    Res = lists:map(
        fun(SpaceId) ->
            SpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
            {ok, SpaceData} = find(<<"data-space">>, [SpaceDirId]),
            SpaceData
        end, SpaceIds),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"data-space">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"data-space">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"data-space">>, _Id, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"data-space">>, _Id) ->
    gui_error:report_error(<<"Not iplemented">>).

