%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs attributes-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_attrs).

-include("modules/fslogic/metadata.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-type file_attributes() :: #file_attr{}.
-type file_details() :: #file_details{}.
-type fs_stats() :: #fs_stats{}.

-export_type([file_attributes/0, file_details/0, fs_stats/0]).

%% API
-export([
    stat/2, get_fs_stats/2, get_details/2,
    get_xattr/4, set_xattr/5, remove_xattr/3, list_xattr/4,
    update_times/5
]).
-export([
    get_transfer_encoding/2, set_transfer_encoding/3,
    get_cdmi_completion_status/2, set_cdmi_completion_status/3, get_mimetype/2,
    set_mimetype/3
]).
-export([get_metadata/5, set_metadata/5, has_custom_metadata/2, remove_metadata/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes (see file_attr.hrl).
%% @end
%%--------------------------------------------------------------------
-spec stat(SessId :: session:id(), FileKey :: lfm:file_key()) ->
    {ok, file_attributes()} | lfm:error_reply().
stat(SessId, FileKey) ->
    case FileKey of
        {path, Path} ->
            remote_utils:call_fslogic(SessId, fuse_request,
                #resolve_guid{path = Path},
                fun(#guid{guid = Guid}) ->
                    stat(SessId, {guid, Guid})
                end);
        {guid, FileGuid} ->
            remote_utils:call_fslogic(SessId, file_request, FileGuid, #get_file_attr{},
                fun(#file_attr{} = Attrs) ->
                    {ok, Attrs}
                end)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns fs_stats() containing support e.g. size and occupied size.
%% @end
%%--------------------------------------------------------------------
-spec get_fs_stats(session:id(), FileKey :: lfm:file_key()) ->
    {ok, fs_stats()} | lfm:error_reply().
get_fs_stats(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(
        SessId,
        fuse_request,
        #get_fs_stats{file_id = FileGuid},
        fun(#fs_stats{} = FsStats) -> {ok, FsStats} end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Returns file details (see file_details.hrl).
%% @end
%%--------------------------------------------------------------------
-spec get_details(session:id(), FileKey :: lfm:file_key()) ->
    {ok, file_details()} | lfm:error_reply().
get_details(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid, #get_file_details{},
        fun(#file_details{} = FileDetails) ->
            {ok, FileDetails}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Changes file timestamps.
%% @end
%%--------------------------------------------------------------------
-spec update_times(session:id(), lfm:file_key(),
    ATime :: file_meta:time(), MTime :: file_meta:time(),
    CTime :: file_meta:time()) -> ok | lfm:error_reply().
update_times(SessId, FileKey, ATime, MTime, CTime) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #update_times{atime = ATime, mtime = MTime, ctime = CTime},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(SessId :: session:id(), FileKey :: lfm:file_key(),
    XattrName :: custom_metadata:name(), boolean()) ->
    {ok, #xattr{}} | lfm:error_reply().
get_xattr(SessId, FileKey, XattrName, Inherited) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_xattr{name = XattrName, inherited = Inherited},
        fun(#xattr{} = Xattr) ->
            {ok, Xattr}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(SessId :: session:id(),
    FileKey :: lfm:file_key(), Xattr :: #xattr{},
    Create :: boolean(), Replace :: boolean()) ->
    ok | lfm:error_reply().
set_xattr(SessId, FileKey, Xattr, Create, Replace) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #set_xattr{xattr = Xattr, create = Create, replace = Replace},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(SessId :: session:id(), FileKey :: lfm:file_key(),
    XattrName :: custom_metadata:name()) ->
    ok | lfm:error_reply().
remove_xattr(SessId, FileKey, XattrName) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #remove_xattr{name = XattrName},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(session:id(), FileUuid :: lfm:file_key(),
    boolean(), boolean()) ->
    {ok, [custom_metadata:name()]} | lfm:error_reply().
list_xattr(SessId, FileKey, Inherited, ShowInternal) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #list_xattr{inherited = Inherited, show_internal = ShowInternal},
        fun(#xattr_list{names = Names}) ->
            {ok, Names}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_encoding(session:id(), lfm:file_key()) ->
    {ok, custom_metadata:transfer_encoding()} | lfm:error_reply().
get_transfer_encoding(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_transfer_encoding{},
        fun(#transfer_encoding{value = Val}) -> {ok, Val} end).

%%--------------------------------------------------------------------
%% @doc
%% Sets encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec set_transfer_encoding(session:id(), lfm:file_key(),
    custom_metadata:transfer_encoding()) ->
    ok | lfm:error_reply().
set_transfer_encoding(SessId, FileKey, Encoding) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #set_transfer_encoding{value = Encoding},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(session:id(), lfm:file_key()) ->
    {ok, custom_metadata:cdmi_completion_status()} | lfm:error_reply().
get_cdmi_completion_status(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_cdmi_completion_status{},
        fun(#cdmi_completion_status{value = Val}) -> {ok, Val} end).

%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(session:id(), lfm:file_key(),
    custom_metadata:cdmi_completion_status()) ->
    ok | lfm:error_reply().
set_cdmi_completion_status(SessId, FileKey, CompletionStatus) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #set_cdmi_completion_status{value = CompletionStatus},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Returns mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(session:id(), lfm:file_key()) ->
    {ok, custom_metadata:mimetype()} | lfm:error_reply().
get_mimetype(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_mimetype{},
        fun(#mimetype{value = Val}) -> {ok, Val} end).

%%--------------------------------------------------------------------
%% @doc
%% Sets mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec set_mimetype(session:id(), lfm:file_key(), custom_metadata:mimetype()) ->
    ok | lfm:error_reply().
set_mimetype(SessId, FileKey, Mimetype) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #set_mimetype{value = Mimetype},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Gets metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(session:id(), lfm:file_key(), custom_metadata:type(), custom_metadata:query(), boolean()) ->
    {ok, custom_metadata:value()} | lfm:error_reply().
get_metadata(SessId, FileKey, Type, Query, Inherited) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #get_metadata{type = Type, query = Query, inherited = Inherited},
        fun(#metadata{value = Value}) -> {ok, Value} end).

%%--------------------------------------------------------------------
%% @doc
%% Sets metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(session:id(), lfm:file_key(), custom_metadata:type(), custom_metadata:value(), custom_metadata:query()) ->
    ok | lfm:error_reply().
set_metadata(SessId, FileKey, Type, Value, Query) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #set_metadata{query = Query, metadata = #metadata{type = Type, value = Value}},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Checks if file has custom metadata defined
%% @end
%%--------------------------------------------------------------------
-spec has_custom_metadata(session:id(), lfm:file_key()) ->
{ok, boolean()} | lfm:error_reply().
has_custom_metadata(SessId, FileKey) ->
    case list_xattr(SessId, FileKey, false, true) of
        {ok, List} ->
            FilteredList = lists:filter(fun
                (?JSON_METADATA_KEY) ->
                    true;
                (?RDF_METADATA_KEY) ->
                    true;
                (<<?CDMI_PREFIX_STR, _/binary>>) ->
                    false;
                (<<?ONEDATA_PREFIX_STR, _/binary>>) ->
                    false;
                (_) ->
                    true
            end, List),
            {ok, FilteredList =/= []};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(session:id(), lfm:file_key(), custom_metadata:type()) ->
    ok | lfm:error_reply().
remove_metadata(SessId, FileKey, Type) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid,
        #remove_metadata{type = Type},
        fun(_) -> ok end).
