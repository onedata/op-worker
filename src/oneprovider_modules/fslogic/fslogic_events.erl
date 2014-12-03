%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: fslogic events handlers and callbacks
%% @end
%% ===================================================================
-module(fslogic_events).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include("fuse_messages_pb.hrl").
-include("registered_names.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([on_file_size_update/4, on_file_meta_update/2]).
-export([handle_event/2]).
-export([push_new_attrs/2]).

%% ===================================================================
%% Triggers
%% ===================================================================

%% on_file_size_update/3
%% ====================================================================
%% @doc Shall be called whenever size of the file has changed.
-spec on_file_size_update(FileUUID :: uuid(), OldFileSize :: non_neg_integer(),
    NewFileSize :: non_neg_integer(), IgnoredFuses :: list()) -> ok.
%% ====================================================================
on_file_size_update(FileUUID, OldFileSize, NewFileSize, IgnoredFuses) ->
    gen_server:call(?Dispatcher_Name, {fslogic, 1, {internal_event, on_file_size_update, {FileUUID, OldFileSize, NewFileSize, IgnoredFuses}}}, timer:seconds(5)).


%% on_file_meta_update/2
%% ====================================================================
%% @doc Shall be called whenever file_meta document of the file has changed.
-spec on_file_meta_update(FileUUID :: uuid(), Doc :: db_doc()) -> ok.
%% ====================================================================
on_file_meta_update(FileUUID, Doc) ->
    gen_server:call(?Dispatcher_Name, {fslogic, 1, {internal_event, on_file_meta_update, {FileUUID, Doc}}}, timer:seconds(5)).

%% ===================================================================
%% Handlers
%% ===================================================================

%% handle_event/2
%% ====================================================================
%% @doc Handles events in fslogic worker context
-spec handle_event(EventType :: atom(), Args :: term()) -> ok.
%% ====================================================================
handle_event(on_file_size_update, {FileUUID, _OldFileSize, _NewFileSize, IgnoredFuses}) ->
    delayed_push_attrs(FileUUID, IgnoredFuses);
handle_event(on_file_meta_update, {FileUUID, _Doc}) ->
    delayed_push_attrs(FileUUID, []);
handle_event(EventType, _Args) ->
    ?warning("Unknown event with type: ~p", [EventType]),
    ok.


%% delayed_push_attrs/1
%% ====================================================================
%% @doc Mark the file's attributes to be updated in fuse clients within
%% next second except clients listed in 'IgnoredFuses' list.
-spec delayed_push_attrs(FileUUID :: uuid(), IgnoredFuses :: list()) -> ok.
%% ====================================================================
delayed_push_attrs(FileUUID, IgnoredFuses) ->
    case ets:lookup(?fslogic_attr_events_state, utils:ensure_binary(FileUUID)) of
        [{_, _TRef}] -> ok;
        [] ->
            TRef = erlang:send_after(500, ?Dispatcher_Name, {timer, {fslogic, 1, {internal_event_handle, push_new_attrs, [FileUUID, IgnoredFuses]}}}),
            ets:insert(?fslogic_attr_events_state, {utils:ensure_binary(FileUUID), TRef}),
            ok
    end.


%% push_new_attrs/2
%% ====================================================================
%% @doc Pushes current file's attributes to all fuses that are currently using this file
-spec push_new_attrs(FileUUID :: uuid(), IgnoredFuses :: list()) ->
    [Result :: ok | {error, Reason :: any()}].
%% ====================================================================
push_new_attrs(FileUUID, IgnoredFuses) ->
   lists:flatten(push_new_attrs4(FileUUID, 0, 100, IgnoredFuses)).
push_new_attrs4(FileUUID, Offset, Count, IgnoredFuses) ->
    ets:delete(?fslogic_attr_events_state, utils:ensure_binary(FileUUID)),
    {ok, FDs} = dao_lib:apply(dao_vfs, list_descriptors, [{by_uuid_n_owner, {utils:ensure_list(FileUUID), ""}}, Count, Offset], 1),
    Fuses0 = lists:map(
        fun(#db_document{record = #file_descriptor{fuse_id = FuseID}}) ->
            FuseID
        end, FDs),
    Fuses1 = lists:usort(Fuses0) -- IgnoredFuses,
    ?debug("Pushing new attributes for file ~p to fuses ~p", [FileUUID, Fuses1]),

    case Fuses1 of
        [] -> [];
        FuseIDs ->
            {ok, #db_document{} = FileDoc} = fslogic_objects:get_file({uuid, FileUUID}),
            Attrs = #fileattr{} = fslogic_req_generic:get_file_attr(FileDoc),

            Results = lists:map(
                fun(FuseID) ->
                    request_dispatcher:send_to_fuse(FuseID, Attrs, "fuse_messages")
                end, FuseIDs),
            [push_new_attrs4(FileUUID, Offset + Count, 100, IgnoredFuses) | Results]
    end.
