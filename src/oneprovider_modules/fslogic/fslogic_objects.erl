%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module consists of DAO objects/records accessors. <br/>
%%       All methods wrap DAO operations while adding some fslogic-specific logic
%%       and/or error translations.
%% @end
%% ===================================================================
-module(fslogic_objects).
-author("Rafal Slota").

-include("registered_names.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").

%% API
-export([get_file/1, get_waiting_file/1, get_file/3, get_waiting_file/3]).
-export([ensure_file_descriptor_exists/4, ensure_file_descriptor_exists_unsafe/4, save_file_descriptor/3, save_new_file_descriptor/4, update_file_descriptor/2, delete_old_descriptors/2]).
-export([get_user/0, get_user/1]).
-export([save_file/1, get_storage/1]).
-export([get_space/1]).

%% ====================================================================
%% API functions
%% ====================================================================


%% get_space/1
%% ====================================================================
%% @doc Convenience wrapper for dao_vfs:get_space_file that accepts wider range of arguments
%%      and returns errors compatible with fslogic's error handler. Also if requested space does not
%%      exist, this method tries to initialize one using GlobarRegistry thus error is returned only if
%%      space does not exists or there was an error during its initialization.
%% @end
-spec get_space(Query) -> {ok, SpaceInfo :: #space_info{}} | {error, {unknown_space_error | initialize_error, Reason :: any()}}
    when Query :: #db_document{}
                | #file{}
                | #space_info{}
                | {uuid, SpaceId}
                | SpaceName,
         SpaceName :: binary(),
         SpaceId :: uuid().
%% ====================================================================
get_space(#db_document{record = Record}) ->
    get_space(Record);
get_space(#file{extensions = Ext}) ->
    {_, #space_info{} = SpaceInfo} = lists:keyfind(?file_space_info_extestion, 1, Ext),
    get_space(SpaceInfo);
get_space(#space_info{} = SpaceInfo) ->
    {ok, SpaceInfo};
get_space({uuid, SpaceId}) ->
    case dao_lib:apply(vfs, get_space_file, [{uuid, SpaceId}], 1) of
        {ok, #db_document{record = #file{} = File}} ->
            get_space(File);
        {error, file_not_found} ->
            try fslogic_spaces:initialize(SpaceId) of
                {ok, #space_info{} = SpaceInfo} ->
                    {ok, SpaceInfo};
                {error, InitReason} ->
                    ?error("Cannot initialize space ~p due to: ~p", [SpaceId, InitReason]),
                    {error, {initialize_error, InitReason}}
            catch
                _Type:Except ->
                    ?error_stacktrace("Cannot initialize space ~p due to: ~p", [SpaceId, Except]),
                    {error, {initialize_error, Except}}
            end;
        {error, Reason} ->
            ?error("Unknown space ~p", [SpaceId]),
            {error, {unknown_space_error, Reason}}
    end;
get_space(SpaceName) ->
    {ok, FileDoc} = dao_lib:apply(vfs, get_space_file, [filename:join(?SPACES_BASE_DIR_NAME, unicode:characters_to_list(SpaceName))], 1),
    get_space(FileDoc).




%% save_file/1
%% ====================================================================
%% @doc Updates given #file{} to DB.
%% @end
-spec save_file(FileDoc :: file_doc()) ->
    {ok, UUID :: uuid()} |
    {error, {failed_to_save_file, {Reason :: any(), FileDoc :: file_doc()}}}.
%% ====================================================================
save_file(FileDoc = #db_document{record = #file{}}) ->
    case dao_lib:apply(dao_vfs, save_file, [FileDoc], fslogic_context:get_protocol_version()) of
        {ok, UUID}      -> {ok, UUID};
        {error, Reason} -> {error, {failed_to_save_file, {Reason, FileDoc}}}
    end.


%% get_storage/1
%% ====================================================================
%% @doc Gets storage document from DB by ID or UUID of document).
%% @end
-spec get_storage({Type :: id | uuid, StorageID :: integer() | uuid()}) ->
    {ok, StorageDoc :: storage_doc()} |
    {error, {failed_to_get_storage, {Reason :: any(), {storage, Type :: atom, StorageID :: integer()}}}}.
%% ====================================================================
get_storage({Type, StorageID}) ->
    case dao_lib:apply(dao_vfs, get_storage, [{Type, StorageID}], fslogic_context:get_protocol_version()) of
        {ok, #db_document{record = #storage_info{}} = SInfo} ->
            {ok, SInfo};
        {error, Reason} ->
            {error, {failed_to_get_storage, {Reason, {storage, Type, StorageID}}}}
    end.


%% get_user/0
%% ====================================================================
%% @doc Gets user associated with current session from DB
%%      If there is no user associated with current session, ROOT user is returned.
%% @end
-spec get_user() -> {ok, UserDoc :: user_doc()} | {error, any()}.
%% ====================================================================
get_user() ->
    get_user(fslogic_context:get_user_query()).


%% get_user/1
%% ====================================================================
%% @doc Gets user associated with given DN
%%      If DN is 'undefined', ROOT user is returned.
%% @end
-spec get_user({dn, DN :: string()} | user_doc() | undefined) -> {ok, UserDoc :: user_doc() | [user_doc()]} | {error, any()}.
%% ====================================================================
get_user(undefined) ->
    {ok, #db_document{uuid = ?CLUSTER_USER_ID, record = #user{logins = [#id_token_login{login = "root", provider_id = internal}], role = admin}}};
get_user(#db_document{record = #user{}} = UserDoc) ->
    {ok, UserDoc};
get_user({Key, Value}) ->
    case user_logic:get_user({Key, Value}) of
        {ok, #db_document{}} = OKRet -> OKRet;
        {ok, UserList} = OKRet when is_list(UserList)  -> OKRet;
        {error, user_not_found} when Key =:= global_id ->
            GRUID = Value,

            fslogic_spaces:sync_all_supported_spaces(),
            {ok, SpaceFiles} = dao_lib:apply(vfs, get_space_files, [{gruid, utils:ensure_binary(GRUID)}], fslogic_context:get_protocol_version()),

            Spaces = [fslogic_utils:file_to_space_info(SpaceFile) || #db_document{record = #file{}} = SpaceFile <- SpaceFiles],

            user_logic:create_partial_user(GRUID, Spaces);
        {error, Reason} ->
            {error, {get_user_error, {Reason, {key, Key}, {value, Value}}}}
    end.


%% get_file/1
%% ====================================================================
%% @doc Gets file info from DB
%% @end
-spec get_file(FullFileName :: file() | file_doc()) -> Result when
    Result :: {ok, FileDoc :: file_doc()} | {error, file_not_found} | {error, any()}.
%% ====================================================================
get_file(#db_document{record = #file{}} = FileDoc) ->
    {ok, FileDoc};
get_file(FullFileName) ->
    get_file(fslogic_context:get_protocol_version(), FullFileName, fslogic_context:get_fuse_id()).


%% get_file/3
%% ====================================================================
%% @doc Gets file info from DB. Context independent version of get_file/1
%% @end
-spec get_file(ProtocolVersion :: term(), FullFileName :: file() | file_doc(), FuseID :: term()) -> Result when
    Result :: {ok, FileDoc :: file_doc()} | {error, file_not_found} | {error, any()}.
%% ====================================================================
get_file(ProtocolVersion, File, _FuseID) when is_tuple(File) ->
    dao_lib:apply(dao_vfs, get_file, [File], ProtocolVersion);
get_file(_ProtocolVersion, FullFileName, _FuseID) ->
    get_file_helper(FullFileName, get_file).


%% get_waiting_file/3
%% ====================================================================
%% @doc Gets file info about file that waits to be created at storage from DB
%% @end
-spec get_waiting_file(FullFileName :: string()) -> Result when
    Result :: term().
%% ====================================================================
get_waiting_file(FullFileName) ->
    get_waiting_file(fslogic_context:get_protocol_version(), FullFileName, fslogic_context:get_fuse_id()).

get_waiting_file(_ProtocolVersion, FullFileName, _FuseID) ->
    get_file_helper(FullFileName, get_waiting_file).

get_file_helper(File, Fun) ->
    get_file_helper(fslogic_context:get_protocol_version(), File, fslogic_context:get_fuse_id(), Fun).

%% get_file_helper/4
%% ====================================================================
%% @doc Gets file info from DB
%% @end
-spec get_file_helper(ProtocolVersion :: term(), File :: string(), FuseID :: string(), Fun :: atom()) -> Result when
    Result :: term().
%% ====================================================================
get_file_helper(ProtocolVersion, File, FuseID, Fun) ->
    ?debug("get_file(File: ~p, FuseID: ~p)", [File, FuseID]),
    case string:tokens(File, "/") of
        [?SPACES_BASE_DIR_NAME, GroupName | _] -> %% Check if group that user is tring to access is avaliable to him
            case fslogic_context:get_user_dn() of %% Internal call, allow all group access
                undefined   -> dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion);
                UserDN      -> %% Check if user has access to this group
                    Teams = user_logic:get_space_names({dn, UserDN}),
                    case lists:member(GroupName, Teams) of %% Does the user belong to the group?
                        true  -> dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion);
                        false -> {error, file_not_found} %% Assume that this file does not exists
                    end
            end;
        _ ->
            dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion)
    end.


%% ensure_file_descriptor_exists/4
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE. Requests are synchronized through dao_worker process tree
%% @end
-spec ensure_file_descriptor_exists(ProtocolVersion :: term(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================
ensure_file_descriptor_exists(ProtocolVersion, Uuid, FuseID, Validity) ->
    MsgId = make_ref(),
    gen_server:call(?Dispatcher_Name, {dao_worker, ProtocolVersion, self(), MsgId, {ensure_file_descriptor_exists, Uuid, FuseID, Validity}}, ?CACHE_REQUEST_TIMEOUT),
    receive
        {worker_answer, MsgId, Resp} -> Resp
    after ?CACHE_REQUEST_TIMEOUT ->
        ?error("Timeout in call to ensure_file_location_exists function in process tree"),
        {error, timeout}
    end.

%% ensure_file_descriptor_exists_unsafe/4
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec ensure_file_descriptor_exists_unsafe(ProtocolVersion :: term(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================
ensure_file_descriptor_exists_unsafe(ProtocolVersion, Uuid, FuseID, Validity) ->
    case FuseID of
        ?CLUSTER_FUSE_ID -> {ok, ok};
        _ ->
            Status = dao_lib:apply(dao_vfs, list_descriptors, [{by_uuid_n_owner, {Uuid, FuseID}}, 10, 0], ProtocolVersion),
            case Status of
                {ok, TmpAns} ->
                    case length(TmpAns) of
                        0 ->
                            {ok, NewUuid} = save_new_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity),
                            case dao_lib:apply(dao_vfs, list_descriptors, [{by_uuid_n_owner, {Uuid, FuseID}}, 10, 0], ProtocolVersion) of
                                {ok, [_]} -> {ok, NewUuid};
                                {ok, List} when length(List) =/= 0 ->
                                    dao_lib:apply(dao_vfs, remove_descriptor, [NewUuid], ProtocolVersion),
                                    case [ Id || #db_document{uuid = Id} <- List, Id =/= NewUuid] of
                                        [Element] -> {ok, Element};
                                        _ ->
                                            ?error("Error: to many file descriptors for file uuid: ~p", [Uuid]),
                                            {error, "Error: too many file descriptors"}
                                    end;
                                Other -> Other
                            end;
                        1 ->
                            [DbDoc] = TmpAns,
                            case save_file_descriptor(ProtocolVersion, DbDoc, Validity) of
                                {ok,Uid} -> {ok,Uid};
                                {error, {save_file_descriptor, {conflict, _}}} -> {ok,DbDoc#db_document.uuid};
                                Other -> Other
                            end;
                        _Many ->
                            ?error("Error: to many file descriptors for file uuid: ~p", [Uuid]),
                            {error, "Error: too many file descriptors"}
                    end;
                _Other -> _Other
            end
    end.


%% save_file_descriptor/3
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), File :: record(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================
save_file_descriptor(ProtocolVersion, FileDescriptorDoc, Validity) ->
    Descriptor = update_file_descriptor(FileDescriptorDoc#db_document.record, Validity),
    case dao_lib:apply(dao_vfs, save_descriptor, [FileDescriptorDoc#db_document{record = Descriptor}], ProtocolVersion) of
        {error, Reason} ->
            {error, {save_file_descriptor, {Reason, Descriptor}}};
        Other -> Other
    end.


%% save_new_file_descriptor/4
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_new_file_descriptor(ProtocolVersion :: term(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================
save_new_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity) ->
    Descriptor = update_file_descriptor(#file_descriptor{file = Uuid, fuse_id = FuseID}, Validity),
    case dao_lib:apply(dao_vfs, save_descriptor, [Descriptor], ProtocolVersion) of
        {error, Reason} ->
            {error, {save_new_file_descriptor, {Reason, Descriptor}}};
        Other -> Other
    end.

%% update_file_descriptor/2
%% ====================================================================
%% @doc Updates descriptor (record, not in DB)
%% @end
-spec update_file_descriptor(Descriptor :: record(),  Validity :: integer()) -> Result when
    Result :: record().
%% ====================================================================
update_file_descriptor(Descriptor, Validity) ->
    {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
    Time = 1000000*Megaseconds + Seconds,
    Descriptor#file_descriptor{create_time = Time, validity_time = Validity}.


%% delete_old_descriptors/2
%% ====================================================================
%% @doc Deletes old descriptors (older than Time)
%% @end
-spec delete_old_descriptors(ProtocolVersion :: term(), Time :: integer()) -> Result when
    Result :: term().
%% ====================================================================
delete_old_descriptors(ProtocolVersion, Time) ->
    Status = dao_lib:apply(dao_vfs, remove_descriptor, [{by_expired_before, Time}], ProtocolVersion),
    %% dao_lib:apply(dao_vfs, remove_attr_watcher, [{by_expired_before, Time}], ProtocolVersion),
    case Status of
        ok ->
            ?info("Old descriptors cleared"),
            ok;
        Other ->
            ?error("Error during clearing old descriptors: ~p", [Other]),
            Other
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
