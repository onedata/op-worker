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
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([
    mkdir/3, mkdir/4,
    create_dir_at_path/3,
    get_children_attrs/4,
    get_child_attr/4,
    get_children_count/2,
    get_files_recursively/4
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec mkdir(session:id(), file_meta:path(), undefined | file_meta:posix_permissions()) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
mkdir(SessId, Path, Mode) ->
    {Name, ParentPath} = filepath_utils:basename_and_parent_dir(Path),

    remote_utils:call_fslogic(SessId, fuse_request, #resolve_guid{path = ParentPath},
        fun(#guid{guid = ParentGuid}) ->
            mkdir(SessId, ParentGuid, Name, Mode)
        end).


-spec mkdir(
    session:id(),
    file_id:file_guid(),
    file_meta:name(),
    undefined | file_meta:posix_permissions()
) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
mkdir(SessId, ParentGuid, Name, undefined) ->
    Mode = op_worker:get_env(default_dir_mode),
    mkdir(SessId, ParentGuid, Name, Mode);
mkdir(SessId, ParentGuid0, Name, Mode) ->
    ParentGuid1 = lfm_file_key:resolve_file_key(
        SessId, ?FILE_REF(ParentGuid0), resolve_symlink
    ),
    remote_utils:call_fslogic(SessId, file_request, ParentGuid1,
        #create_dir{name = Name, mode = Mode},
        fun(#dir{guid = DirGuid}) ->
            {ok, DirGuid}
        end).


-spec create_dir_at_path(session:id(), fslogic_worker:file_guid(), file_meta:path()) ->
    {ok, #file_attr{}} | lfm:error_reply().
create_dir_at_path(SessId, ParentGuid0, Path) ->
    ParentGuid1 = lfm_file_key:resolve_file_key(
        SessId, ?FILE_REF(ParentGuid0), resolve_symlink
    ),
    
    remote_utils:call_fslogic(SessId, file_request, ParentGuid1,
        #create_path{path = Path},
        fun(#file_attr{} = FileAttr) ->
            {ok, FileAttr}
        end
    ).


%%--------------------------------------------------------------------
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each directory children
%% starting with Offset-th entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(session:id(), lfm:file_key(), file_listing:options(), [file_attr:attribute()]) ->
    {ok, [#file_attr{}], file_listing:pagination_token()} | lfm:error_reply().
get_children_attrs(SessId, FileKey, ListingOpts, Attributes) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children_attrs{
            listing_options = ListingOpts,
            attributes = Attributes
        },
        fun(#file_children_attrs{
            child_attrs = Attrs,
            pagination_token = ListingToken
        }) ->
            {ok, Attrs, ListingToken}
        end).



-spec get_child_attr(session:id(), file_id:file_guid(), file_meta:name(), [file_attr:attribute()]) ->
    {ok, #file_attr{}} | lfm:error_reply().
get_child_attr(SessId, ParentGuid0, ChildName, Attributes)  ->
    ParentGuid1 = lfm_file_key:resolve_file_key(
        SessId, ?FILE_REF(ParentGuid0), resolve_symlink
    ),
    remote_utils:call_fslogic(SessId, file_request, ParentGuid1,
        #get_child_attr{name = ChildName, attributes = Attributes},
        fun(Attrs) -> {ok, Attrs} end
    ).


-spec get_children_count(session:id(), lfm:file_key()) ->
    {ok, non_neg_integer()} | lfm:error_reply().
get_children_count(SessId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),

    case count_children(SessId, FileGuid) of
        {ok, ChildrenNum} -> {ok, ChildrenNum};
        {error, Err} -> {error, Err}
    end.


-spec get_files_recursively(
    session:id(), 
    lfm:file_key(), 
    dir_req:recursive_listing_opts(),
    [file_attr:attribute()]
) ->
    {ok, [recursive_file_listing_node:entry()], [file_meta:path()], recursive_listing:pagination_token()}.
get_files_recursively(SessId, FileKey, Options, Attributes) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),
    
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_recursive_file_list{
            listing_options = Options,
            attributes = Attributes
        },
        fun(#recursive_listing_result{
            entries = Result,
            inaccessible_paths = InaccessiblePaths,
            pagination_token = PaginationToken
        }) ->
            {ok, Result, InaccessiblePaths, PaginationToken}
        end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec count_children(session:id(), fslogic_worker:file_guid()) ->
    {ok, non_neg_integer()} | lfm:error_reply().
count_children(SessId, FileGuid) ->
    count_children(SessId, FileGuid, #{tune_for_large_continuous_listing => true}, 0).


%% @private
-spec count_children( session:id(), fslogic_worker:file_guid(),
    file_listing:options(), Acc :: non_neg_integer()) ->
    {ok, non_neg_integer()} | lfm:error_reply().
count_children(SessId, FileGuid, ListOpts, Acc) ->
    case get_children_attrs(SessId, ?FILE_REF(FileGuid), ListOpts, [guid]) of
        {ok, List, ListingPaginationToken} ->
            case file_listing:is_finished(ListingPaginationToken) of
                true ->
                    {ok, Acc + length(List)};
                false ->
                    ListOpts2 = ListOpts#{
                        pagination_token => ListingPaginationToken
                    },
                    count_children(SessId, FileGuid, ListOpts2, Acc + length(List))
            end;
        {error, _} = Error ->
            Error
    end.
