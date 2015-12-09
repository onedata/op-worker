%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module responsible for pushing new file's information to sessions.
%%%      @todo: Should be replaced with event system
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_event).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_file_attr_update/2, emit_file_location_update/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_update(fslogic_worker:file(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_update(FileEntry, ExcludedSessions) ->
    case logical_file_manager:stat(?ROOT_SESS_ID, FileEntry) of
        {ok, #file_attr{} = FileAttr} ->
            ?debug("Sending new attributes for file ~p to all sessions except ~p",
                [FileEntry, ExcludedSessions]),
            event:emit(#event{object = #update_event{object = FileAttr}}, {exclude, ExcludedSessions});
        {error, Reason} ->
            ?error("Unable to get new attributes for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_update(fslogic_worker:file(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_location_update(FileEntry, ExcludedSessions) ->
    try
        {ok, #document{} = File} = file_meta:get(FileEntry),
        #document{value = #file_location{} = FileLocation} = fslogic_utils:get_local_file_location(File),
        event:emit(#event{object = #update_event{object = FileLocation}}, {exclude, ExcludedSessions})
    catch
        _:Reason ->
            ?error_stacktrace("Unable to push new location for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================