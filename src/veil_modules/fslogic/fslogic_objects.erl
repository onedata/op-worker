%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Write me !
%% @end
%% ===================================================================
-module(fslogic_objects).
-author("Rafal Slota").

-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

%% API
-export([get_file/1, get_waiting_file/1, get_file/3, get_waiting_file/3]).
-export([save_file_descriptor/3, save_file_descriptor/4, save_new_file_descriptor/4, update_file_descriptor/2, delete_old_descriptors/2]).
-export([get_user/0]).
-export([save_file/1, get_storage/1]).

%% ====================================================================
%% API functions
%% ====================================================================

save_file(FileDoc = #veil_document{record = #file{}}) ->
    case dao_lib:apply(dao_vfs, save_file, [FileDoc], fslogic_context:get_protocol_version()) of
        {ok, UUID}      -> {ok, UUID};
        {error, Reason} -> {error, {failed_to_save_file, {Reason, FileDoc}}}
    end.

get_storage({id, StorageID}) ->
    case dao_lib:apply(dao_vfs, get_storage, [{uuid, StorageID}], fslogic_context:get_protocol_version()) of
        {ok, #veil_document{record = #storage_info{}} = SInfo} ->
            {ok, SInfo};
        {error, Reason} ->
            {error, {failed_to_get_storage, {Reason, {storage_id, StorageID}}}}
    end.

get_user(#veil_document{record = #user{}} = UserDoc) ->
    {ok, UserDoc};
get_user({dn, UserDN}) ->
    case UserDN of
        undefined -> {error, get_user_id_error};
        DN ->
            user_logic:get_user({dn, DN})
    end.
get_user() ->
    get_user({dn, fslogic_context:get_user_dn()}).


%% get_file/3
%% ====================================================================
%% @doc Gets file info from DB
%% @end
-spec get_file(FullFileName :: string()) -> Result when
    Result :: term().
%% ====================================================================
get_file(#veil_document{record = #file{}} = FileDoc) ->
    {ok, FileDoc};
get_file(FullFileName) ->
    get_file(fslogic_context:get_protocol_version(), FullFileName, fslogic_context:get_fuse_id()).

get_file(_ProtocolVersion, FullFileName, _FuseID) ->
    case get_file_helper(FullFileName, get_file) of
        {error, file_not_found} -> {error, file_not_found};
        {error, Reason} -> {error, Reason};
        {ok, FileDoc} -> {ok, FileDoc}
    end.

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
    case get_file_helper(FullFileName, get_waiting_file) of
        {error, file_not_found} -> {error, file_not_found};
        {error, Reason} -> {error, Reason};
        {ok, FileDoc} -> {ok, FileDoc}
    end.

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
        [?GROUPS_BASE_DIR_NAME, GroupName | _] -> %% Check if group that user is tring to access is avaliable to him
            case get(user_dn) of %% Internal call, allow all group access
                undefined   -> dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion);
                UserDN      -> %% Check if user has access to this group
                    Teams = user_logic:get_team_names({dn, UserDN}),
                    case lists:member(GroupName, Teams) of %% Does the user belong to the group?
                        true  -> dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion);
                        false -> {error, file_not_found} %% Assume that this file does not exists
                    end
            end;
        _ ->
            dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion)
    end.


%% save_file_descriptor/3
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), File :: record(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================

save_file_descriptor(ProtocolVersion, File, Validity) ->
    Descriptor = update_file_descriptor(File#veil_document.record, Validity),
    dao_lib:apply(dao_vfs, save_descriptor, [File#veil_document{record = Descriptor}], ProtocolVersion).


%% save_file_descriptor/4
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================

save_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity) ->
    case FuseID of
        ?CLUSTER_FUSE_ID -> {ok, ok};
        _ ->
            Status = dao_lib:apply(dao_vfs, list_descriptors, [{by_uuid_n_owner, {Uuid, FuseID}}, 10, 0], ProtocolVersion),
            case Status of
                {ok, TmpAns} ->
                    case length(TmpAns) of
                        0 ->
                            save_new_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity);
                        1 ->
                            [VeilDoc | _] = TmpAns,
                            save_file_descriptor(ProtocolVersion, VeilDoc, Validity);
                        _Many ->
                            lager:error([{mod, ?MODULE}], "Error: to many file descriptors for file uuid: ~p", [Uuid]),
                            {error, "Error: too many file descriptors"}
                    end;
                _Other -> _Other
            end
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
    dao_lib:apply(dao_vfs, save_descriptor, [Descriptor], ProtocolVersion).

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
    case Status of
        ok ->
            lager:info([{mod, ?MODULE}], "Old descriptors cleared"),
            ok;
        Other ->
            lager:error([{mod, ?MODULE}], "Error during clearing old descriptors: ~p", [Other]),
            Other
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
