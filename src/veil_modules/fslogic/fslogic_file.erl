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
-module(fslogic_file).
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
-export([normalize_file_type/2]).
-export([update_file_size/1, update_file_size/2, get_real_file_size/1, get_file_owner/1]).

%% ====================================================================
%% API functions
%% ====================================================================

get_file_owner(#file{} = File) ->
    case user_logic:get_user({uuid, File#file.uid}) of
        {ok, #veil_document{record = #user{}} = UserDoc} ->
            {user_logic:get_login(UserDoc), list_to_integer(UserDoc#veil_document.uuid)};
        {error, UError} ->
            lager:error("Owner of file ~p not found due to error: ~p", [File, UError]),
            {"", -1}
    end;
get_file_owner(FilePath) ->
    {ok, #veil_document{record = #file{} = File}} = fslogic_objects:get_file(FilePath),
    get_file_owner(File).

get_file_local_location(#veil_document{record = #file{} = File}) ->
    get_file_local_location(File);
get_file_local_location(File = #file{location = #file_location{} = Location}) ->
    Location.

get_real_file_size(#file{type = ?REG_TYPE} = File) ->
    FileLoc = get_file_local_location(File#file.location),
    {ok, #veil_document{record = Storage}} = fslogic_objects:get_storage({id, FileLoc#file_location.storage_id}),

    {SH, File_id} = fslogic_utils:get_sh_and_id(?CLUSTER_FUSE_ID, Storage, FileLoc#file_location.file_id),
    case veilhelpers:exec(getattr, SH, [File_id]) of
        {0, #st_stat{st_size = ST_Size} = _Stat} ->

            ST_Size;
        {Errno, _} ->
            lager:error("Cannot fetch attributes for file: ~p, errno: ~p", [File, Errno]),
            0
    end;
get_real_file_size(#file{}) ->
    0;
get_real_file_size(FileDocOrPath) ->
    {ok, #veil_document{record = #file{} = File}} = fslogic_objects:get_file(FileDocOrPath),
    get_real_file_size(File).

update_file_size(#file{} = File) ->
    Size = get_real_file_size(File),
    update_file_size(File, Size).

update_file_size(File, Size) ->
    fslogic_utils:update_meta_attr(File, size, Size).



normalize_file_type(protocol, ?DIR_TYPE) ->
    "DIR";
normalize_file_type(protocol, ?REG_TYPE) ->
    "REG";
normalize_file_type(protocol, ?LNK_TYPE) ->
    "LNK";
normalize_file_type(protocol, _) ->
    "UNK";
normalize_file_type(internal, "DIR") ->
    ?DIR_TYPE;
normalize_file_type(internal, "REG") ->
    ?REG_TYPE;
normalize_file_type(internal, "LNK") ->
    ?LNK_TYPE;
normalize_file_type(internal, Type) ->
    throw({unknown_file_type, Type}).

%% ====================================================================
%% Internal functions
%% ====================================================================
