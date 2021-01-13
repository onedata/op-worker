%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs directory-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_dirs).

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([
    mkdir/2, mkdir/3, mkdir/4,
    get_children/4, get_children/5, get_children/6,
    get_children_attrs/4, get_children_attrs/5, get_child_attr/3,
    get_children_details/5,
    get_children_count/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec mkdir(SessId :: session:id(), Path :: file_meta:path()) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | lfm:error_reply().
mkdir(SessId, Path) ->
    mkdir(SessId, Path, undefined).


-spec mkdir(SessId :: session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | lfm:error_reply().
mkdir(SessId, Path, Mode) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(Path),
    remote_utils:call_fslogic(SessId, fuse_request, #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            mkdir(SessId, ParentGuid, Name, Mode)
        end).


-spec mkdir(SessId :: session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | lfm:error_reply().
mkdir(SessId, ParentGuid, Name, undefined) ->
    {ok, Mode} = application:get_env(?APP_NAME, default_dir_mode),
    mkdir(SessId, ParentGuid, Name, Mode);
mkdir(SessId, ParentGuid, Name, Mode) ->
    remote_utils:call_fslogic(SessId, file_request, ParentGuid,
        #create_dir{name = Name, mode = Mode},
        fun(#dir{guid = DirGuid}) ->
            {ok, DirGuid}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Gets {Guid, Name} for each directory children starting with Offset-th
%% entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer()
) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}]} | lfm:error_reply().
get_children(SessId, FileKey, Offset, Limit) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children{offset = Offset, size = Limit},
        fun(#file_children{child_links = List}) ->
            {ok, [{Guid_, FileName} || #child_link{guid = Guid_, name = FileName} <- List]}
        end).


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_children(SessId, FileKey, Offset, Limit, Token, undefined).
%% @end
%%--------------------------------------------------------------------
-spec get_children(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer(),
    Token :: undefined | binary()
) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}], Token :: binary(),
        IsLast :: boolean()} | lfm:error_reply().
get_children(SessId, FileKey, Offset, Limit, Token) ->
    get_children(SessId, FileKey, Offset, Limit, Token, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Gets {Guid, Name} for each directory children starting with Offset-th
%% from specified StartId or Token entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children(session:id(), fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer(),
    Token :: undefined | binary(),
    StartId :: undefined | file_meta:name()
) ->
    {ok, [{file_id:file_guid(), file_meta:name()}], Token :: binary(), IsLast :: boolean()} |
    lfm:error_reply().
get_children(SessId, FileKey, Offset, Limit, Token, StartId) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    GetChildrenReq = #get_file_children{
        offset = Offset,
        size = Limit,
        index_token = Token,
        index_startid = StartId
    },
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        GetChildrenReq,
        fun(#file_children{child_links = List, index_token = Token2, is_last = IL}) ->
            {ok, [{Guid_, FileName}
                || #child_link{guid = Guid_, name = FileName} <- List], Token2, IL}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each directory children
%% starting with Offset-th entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer()
) ->
    {ok, [#file_attr{}]} | lfm:error_reply().
get_children_attrs(SessId, FileKey, Offset, Limit) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children_attrs{offset = Offset, size = Limit},
        fun(#file_children_attrs{child_attrs = Attrs}) ->
            {ok, Attrs}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each directory children
%% starting with Offset-th from specified Token entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer(),
    Token :: undefined | binary()
) ->
    {ok, [#file_attr{}], Token :: binary(), IsLast :: boolean()} |
    lfm:error_reply().
get_children_attrs(SessId, FileKey, Offset, Limit, Token) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children_attrs{offset = Offset, size = Limit, index_token = Token},
        fun(#file_children_attrs{child_attrs = Attrs, index_token = Token2, is_last = IL}) ->
            {ok, Attrs, Token2, IL}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Gets attribute of a child with given name.
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(
    session:id(),
    ParentGuid :: fslogic_worker:file_guid(),
    ChildName :: file_meta:name()
) ->
    {ok, #file_attr{}} | lfm:error_reply().
get_child_attr(SessId, ParentGuid, ChildName)  ->
    remote_utils:call_fslogic(SessId, file_request, ParentGuid,
        #get_child_attr{name = ChildName},
        fun(Attrs) ->
            {ok, Attrs}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Gets file details (see file_details.hrl) for each directory children
%% starting with Offset-th from specified StartId entry and up to Limit
%% of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_details(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer(),
    StartId :: undefined | file_meta:name()
) ->
    {ok, [lfm_attrs:file_details()], IsLast :: boolean()} | lfm:error_reply().
get_children_details(SessId, FileKey, Offset, Limit, StartId) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children_details{
            offset = Offset,
            size = Limit,
            index_startid = StartId
        },
        fun(#file_children_details{child_details = ChildrenInfo, is_last = IL}) ->
            {ok, ChildrenInfo, IL}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns number of children of a directory.
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path()
) ->
    {ok, ChildrenCount :: integer()} | lfm:error_reply().
get_children_count(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    case count_children(SessId, FileGuid, 0) of
        {error, Err} -> {error, Err};
        ChildrenNum -> {ok, ChildrenNum}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Counts all children of a directory, by listing them in chunks as long
%% as possible
%% @end
%%--------------------------------------------------------------------
-spec count_children(SessId :: session:id(), FileGuid :: fslogic_worker:file_guid(),
    Acc :: non_neg_integer()) ->
    non_neg_integer() | lfm:error_reply().
count_children(SessId, FileGuid, Acc) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case get_children(SessId, {guid, FileGuid}, Acc, Chunk) of
        {ok, List} ->
            case length(List) of
                Chunk -> count_children(SessId, FileGuid, Acc + Chunk);
                N -> Acc + N
            end;
        {error, _} = Error ->
            Error
    end.
