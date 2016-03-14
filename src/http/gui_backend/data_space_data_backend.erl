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
%%% the file model used in Ember application.
%%% THIS IS A PROTOTYPE AND AN EXAMPLE OF IMPLEMENTATION.
%%% @end
%%%-------------------------------------------------------------------
-module(data_space_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").

-compile([export_all]).

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([init/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

-define(DEFAULT_SPACE_KEY, default_space).

%% Convenience macro to log a debug level log dumping given variable.
-define(log_debug(_Arg),
    ?alert("~s", [str_utils:format("DATA_SPACE_BACKEND: ~s: ~p", [??_Arg, _Arg])])
).

init() ->
    ?log_debug({websocket_init, g_session:get_session_id()}),
    SessionId = g_session:get_session_id(),
    {ok, #document{value = #session{auth = Auth}}} = session:get(SessionId),
    #auth{macaroon = Mac, disch_macaroons = DMacs} = Auth,
    {ok, DefaultSpace} = oz_users:get_default_space({user, {Mac, DMacs}}),
    ?dump(<<"/spaces/", DefaultSpace/binary>>),
    {ok, #file_attr{uuid = DefaultSpaceId}} = logical_file_manager:stat(
        SessionId, {path, <<"/spaces/", DefaultSpace/binary>>}),
    g_session:put_value(?DEFAULT_SPACE_KEY, DefaultSpaceId),
    ok.

%% Called when ember asks for a particular dataSpace
find(<<"data-space">>, [SpaceId]) ->
    SessionId = g_session:get_session_id(),
    {ok, #file_attr{name = SpaceName}} = logical_file_manager:stat(
        SessionId, {uuid, SpaceId}),
    SpaceDirId = space_id_to_space_dir(SpaceId),
    Res = space_record(SpaceId, SpaceDirId, SpaceName),
    ?log_debug({find, Res}),
    {ok, Res}.

%% Called when ember asks for all files - not implemented, because we don't
%% want to return all files...
find_all(<<"data-space">>) ->
    SessionId = g_session:get_session_id(),
    {ok, SpaceDirs} = logical_file_manager:ls(SessionId,
        {path, <<"/spaces">>}, 0, 1000),
    ?log_debug(SpaceDirs),
    Res = lists:map(
        fun({SpaceDirId, SpaceName}) ->
            SpaceId = space_dir_to_space_id(SpaceDirId),
            space_record(SpaceId, SpaceDirId, SpaceName)
        end, SpaceDirs),
    ?log_debug(Res),
    {ok, Res}.


%% Called when ember asks for file mathcing given query
find_query(<<"data-space">>, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to create a record
create_record(<<"data-space">>, _Data) ->
    {error, not_iplemented}.

update_record(<<"data-space">>, _Id, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to delete a record
delete_record(<<"data-space">>, _Id) ->
    {error, not_iplemented}.


space_record(SpaceId, SpaceDirId, SpaceName) ->
    DefaultSpaceId = g_session:get_value(?DEFAULT_SPACE_KEY),
    [
        {<<"id">>, SpaceId},
        {<<"name">>, SpaceName},
        {<<"isDefault">>, SpaceDirId =:= DefaultSpaceId},
        {<<"rootDir">>, SpaceDirId}
    ].



space_dir_to_space_id(SpaceDirId) ->
    <<"space#", SpaceDirId/binary>>.


space_id_to_space_dir(SpaceId) ->
    <<"space#", SpaceDirId/binary>> = SpaceId,
    SpaceDirId.