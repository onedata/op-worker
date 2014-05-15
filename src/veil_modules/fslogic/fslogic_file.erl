%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides convenience methods that helps with managing #file{} record.
%%       It also provides some abstract getters/setters for some #file{} record in case further changes.
%% @end
%% ===================================================================
-module(fslogic_file).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("logging.hrl").

%% API
-export([normalize_file_type/2]).
-export([update_file_size/1, update_file_size/2, get_real_file_size/1, get_file_owner/1, get_file_local_location/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% get_file_owner/1
%% ====================================================================
%% @doc Fetches owner's username and UID for given file.
%%      Returns {"", -1} on error.
-spec get_file_owner(File :: file_doc() | file_info() | file()) ->
    {Login :: string(), UID :: integer()} |
    {[], -1}.
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


%% get_file_local_location/1
%% ====================================================================
%% @doc Fetches local #file_location{} from #file{} record.
%%      #file_location{} shall never be accessed directly since this could be subject to change.
-spec get_file_local_location(File :: file_doc() | file_info()) -> #file_location{}.
%% ====================================================================
get_file_local_location(#veil_document{record = #file{} = File}) ->
    get_file_local_location(File);
get_file_local_location(_File = #file{location = #file_location{} = LocationField}) ->
    get_file_local_location(LocationField);
get_file_local_location(#file_location{} = FLoc) ->
    FLoc.


%% get_real_file_size/1
%% ====================================================================
%% @doc Fetches real file size from underlying storage. Returns 0 for non-regular file.
%%      Also errors are silently dropped (return value 0).
-spec get_real_file_size(File :: file() | file_doc() | file_info()) -> FileSize :: non_neg_integer().
%% ====================================================================
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


%% update_file_size/1
%% ====================================================================
%% @doc Updates file size based on it's real size on underlying storage. Whether this call is asynchronous or not depends on
%%      fslogic_meta:update_meta_attr implementation. <br/>
%%      Does nothing if given file_info() corresponds to non-regular file.
-spec update_file_size(File :: file_info()) ->
    UpdatedFile :: file_info().
%% ====================================================================
update_file_size(#file{type = ?REG_TYPE} = File) ->
    Size = get_real_file_size(File),
    update_file_size(File, Size);
update_file_size(#file{} = File) ->
    File.


%% update_file_size/2
%% ====================================================================
%% @doc Sets file size to given value. Whether this call is asynchronous or not depends on
%%      fslogic_meta:update_meta_attr implementation.
-spec update_file_size(File :: file_info(), Size :: non_neg_integer()) ->
    UpdatedFile :: file_info().
%% ====================================================================
update_file_size(#file{} = File, Size) when Size >= 0 ->
    fslogic_meta:update_meta_attr(File, size, Size).



%% normalize_file_type/2
%% ====================================================================
%% @doc Translates given file type into internal or protocol representation
%%      (types file_type() and file_type_protocol() respectively) <br/>
%%      This method can and should be used in order to ensure that given file_type
%%      has requested format.
-spec normalize_file_type(protocol | internal, file_type() | file_type_protocol()) -> file_type() | file_type_protocol().
%% ====================================================================
normalize_file_type(protocol, ?DIR_TYPE) ->
    ?DIR_TYPE_PROT;
normalize_file_type(protocol, ?REG_TYPE) ->
    ?REG_TYPE_PROT;
normalize_file_type(protocol, ?LNK_TYPE) ->
    ?LNK_TYPE_PROT;
normalize_file_type(protocol, ?DIR_TYPE_PROT) ->
    ?DIR_TYPE_PROT;
normalize_file_type(protocol, ?REG_TYPE_PROT) ->
    ?REG_TYPE_PROT;
normalize_file_type(protocol, ?LNK_TYPE_PROT) ->
    ?LNK_TYPE_PROT;
normalize_file_type(protocol, Type) ->
    ?error("Unknown file type: ~p", [Type]),
    throw({unknown_file_type, Type});
normalize_file_type(internal, ?DIR_TYPE_PROT) ->
    ?DIR_TYPE;
normalize_file_type(internal, ?REG_TYPE_PROT) ->
    ?REG_TYPE;
normalize_file_type(internal, ?LNK_TYPE_PROT) ->
    ?LNK_TYPE;
normalize_file_type(internal, ?DIR_TYPE) ->
    ?DIR_TYPE;
normalize_file_type(internal, ?REG_TYPE) ->
    ?REG_TYPE;
normalize_file_type(internal, ?LNK_TYPE) ->
    ?LNK_TYPE;
normalize_file_type(internal, Type) ->
    ?error("Unknown file type: ~p", [Type]),
    throw({unknown_file_type, Type}).

%% ====================================================================
%% Internal functions
%% ====================================================================
