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
-include("modules/fslogic/file_attr.hrl").

-type file_attributes() :: #file_attr{}.
-type fs_stats() :: #fs_stats{}.

-export_type([file_attributes/0, fs_stats/0]).

%% API
-export([
    stat/3,
    get_fs_stats/2,
    get_references/2,
    resolve_symlink/2,
    update_times/5
]).
-export([
    list_xattr/4,
    set_xattr/5,
    get_xattr/4,
    remove_xattr/3
]).
-export([has_custom_metadata/2]).

-type success_callback() :: fun((term()) -> term()).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes (see file_attr.hrl).
%% @end
%%--------------------------------------------------------------------
-spec stat(SessId :: session:id(), lfm:file_key(), [onedata_file:attr_name()]) ->
    {ok, file_attributes()} | lfm:error_reply().
stat(SessId, FileKey, Attributes) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    
    remote_utils:call_fslogic(
        SessId, file_request, FileGuid, #get_file_attr{attributes = Attributes},
        fun(#file_attr{} = Attrs) -> {ok, Attrs} end
    ).


%%--------------------------------------------------------------------
%% @doc
%% Returns fs_stats() containing support e.g. size and occupied size.
%% @end
%%--------------------------------------------------------------------
-spec get_fs_stats(session:id(), lfm:file_key()) ->
    {ok, fs_stats()} | lfm:error_reply().
get_fs_stats(SessId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(
        SessId,
        fuse_request,
        #get_fs_stats{file_id = FileGuid},
        fun(#fs_stats{} = FsStats) -> {ok, FsStats} end
    ).


-spec get_references(session:id(), lfm:file_key()) ->
    {ok, [file_id:file_guid()]} | lfm:error_reply().
get_references(SessId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(
        SessId, file_request, FileGuid, #get_file_references{},
        fun(#file_references{references = References}) -> {ok, References} end
    ).


-spec resolve_symlink(session:id(), lfm:file_key()) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
resolve_symlink(SessId, FileKey) ->
    {ok, FileGuid} = lfm_file_key:ensure_guid(SessId, FileKey),

    remote_utils:call_fslogic(
        SessId,
        file_request,
        FileGuid,
        #resolve_symlink{},
        fun(#guid{guid = TargetGuid}) -> {ok, TargetGuid} end
    ).


-spec update_times(
    session:id(),
    lfm:file_key(),
    ATime :: times:time(),
    MTime :: times:time(),
    CTime :: times:time()
) ->
    ok | lfm:error_reply().
update_times(SessId, FileKey, ATime, MTime, CTime) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),

    remote_utils:call_fslogic(
        SessId, file_request, FileGuid,
        #update_times{atime = ATime, mtime = MTime, ctime = CTime},
        fun(_) -> ok end
    ).


-spec list_xattr(session:id(), lfm:file_key(), boolean(), boolean()) ->
    {ok, [onedata_file:xattr_name()]} | lfm:error_reply().
list_xattr(SessId, FileKey, Inherited, ShowInternal) ->
    call_xattr(
        SessId, FileKey,
        #list_xattr{inherited = Inherited, show_internal = ShowInternal},
        fun(#xattr_list{names = Names}) -> {ok, Names} end
    ).


-spec set_xattr(session:id(), lfm:file_key(), #xattr{}, boolean(), boolean()) ->
    ok | lfm:error_reply().
set_xattr(SessId, FileKey, Xattr, Create, Replace) ->
    call_xattr(
        SessId, FileKey,
        #set_xattr{xattr = Xattr, create = Create, replace = Replace},
        fun(_) -> ok end
    ).


-spec get_xattr(session:id(), lfm:file_key(), onedata_file:xattr_name(), boolean()) ->
    {ok, #xattr{}} | lfm:error_reply().
get_xattr(SessId, FileKey, XattrName, Inherited) ->
    call_xattr(
        SessId, FileKey,
        #get_xattr{name = XattrName, inherited = Inherited},
        fun(#xattr{} = Xattr) -> {ok, Xattr} end
    ).


-spec remove_xattr(session:id(), lfm:file_key(), onedata_file:xattr_name()) ->
    ok | lfm:error_reply().
remove_xattr(SessId, FileKey, XattrName) ->
    call_xattr(SessId, FileKey, #remove_xattr{name = XattrName}, fun(_) -> ok end).


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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec call_xattr(session:id(), lfm:file_key(), file_request_type(), success_callback()) ->
    term().
call_xattr(SessId, FileKey, Request, SuccessCallback) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),
    remote_utils:call_fslogic(SessId, file_request, Guid, Request, SuccessCallback).
