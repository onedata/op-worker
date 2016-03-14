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
-module(file_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").

-compile([export_all]).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([init/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

%% Convenience macro to log a debug level log dumping given variable.
-define(log_debug(_Arg),
    ?alert("~s", [str_utils:format("FILE_BACKEND: ~s: ~p", [??_Arg, _Arg])])
).


init() ->
    ?log_debug({websocket_init, g_session:get_session_id()}),
    ok.


%% Called when ember asks for a file (or a dir, which is treated as a file)
find(<<"file">>, [FileId]) ->
    SessionId = g_session:get_session_id(),
    SpacesDirUUID = get_spaces_dir_uuid(),
    ParentUUID = case get_parent(FileId) of
        SpacesDirUUID ->
            null;
        Other ->
            Other
    end,
    {ok, #file_attr{name = Name, type = TypeAttr}} =
        logical_file_manager:stat(SessionId, {uuid, FileId}),
    Type = case TypeAttr of
        ?DIRECTORY_TYPE -> <<"dir">>;
        _ -> <<"file">>
    end,
    Children = case Type of
        <<"file">> ->
            [];
        <<"dir">> ->
            {ok, Chldrn} = logical_file_manager:ls(
                SessionId, {uuid, FileId}, 0, 1000),
            Chldrn
    end,
    ChildrenIds = [ChId || {ChId, _} <- Children],
    Res = [
        {<<"id">>, FileId},
        {<<"name">>, Name},
        {<<"type">>, Type},
        {<<"parent">>, ParentUUID},
        {<<"children">>, ChildrenIds}
    ],
    ?log_debug({find, Res}),
    {ok, Res}.

%% Called when ember asks for all files - not implemented, because we don't
%% want to return all files...
find_all(<<"file">>) ->
    {error, not_iplemented}.


%% Called when ember asks for file mathcing given query
find_query(<<"file">>, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to create a record
create_record(<<"file">>, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to update a record
update_record(<<"file">>, _Id, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to delete a record
delete_record(<<"file">>, _Id) ->
    {error, not_iplemented}.



% ------------------------------------------------------------
% Internal
% ------------------------------------------------------------

get_parent(UUID) ->
    ?dump({get_parent, UUID}),
    SessionId = g_session:get_session_id(),
    {ok, ParentUUID} = logical_file_manager:get_parent(
        SessionId, {uuid, UUID}),
    case logical_file_manager:get_file_path(SessionId, ParentUUID) of
        {ok, <<"/spaces">>} ->
            get_spaces_dir_uuid();
        _ ->
            ParentUUID
    end.


get_spaces_dir_uuid() ->
    SessionId = g_session:get_session_id(),
    {ok, #file_attr{uuid = SpacesDirUUID}} = logical_file_manager:stat(
        SessionId, {path, <<"/spaces">>}),
    SpacesDirUUID.