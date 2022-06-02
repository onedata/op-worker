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
    get_children/3,
    get_children_attrs/5,
    get_child_attr/3,
    get_children_details/3,
    get_children_count/2,
    get_files_recursively/3,
    browse_dir_stats/4
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


%%--------------------------------------------------------------------
%% @doc
%% Gets {Guid, Name} for each directory children starting with Offset-th
%% entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children(session:id(), lfm:file_key(), file_listing:options()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}], file_listing:pagination_token()} | lfm:error_reply().
get_children(SessId, FileKey, ListingOpts) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children{
            listing_options = ListingOpts
        },
        fun(#file_children{
            child_links = List,
            pagination_token = ListingToken
        }) ->
            Children = [{Guid, FileName} || #child_link{guid = Guid, name = FileName} <- List],
            {ok, Children, ListingToken}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each directory children
%% starting with Offset-th entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(session:id(), lfm:file_key(), file_listing:options(), boolean(), boolean()) ->
    {ok, [#file_attr{}], file_listing:pagination_token()} | lfm:error_reply().
get_children_attrs(SessId, FileKey, ListingOpts, IncludeReplicationStatus, IncludeHardlinkCount) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children_attrs{
            listing_options = ListingOpts,
            include_replication_status = IncludeReplicationStatus,
            include_link_count = IncludeHardlinkCount
        },
        fun(#file_children_attrs{
            child_attrs = Attrs,
            pagination_token = ListingToken
        }) ->
            {ok, Attrs, ListingToken}
        end).



-spec get_child_attr(session:id(), file_id:file_guid(), file_meta:name()) ->
    {ok, #file_attr{}} | lfm:error_reply().
get_child_attr(SessId, ParentGuid0, ChildName)  ->
    ParentGuid1 = lfm_file_key:resolve_file_key(
        SessId, ?FILE_REF(ParentGuid0), resolve_symlink
    ),
    remote_utils:call_fslogic(SessId, file_request, ParentGuid1,
        #get_child_attr{name = ChildName},
        fun(Attrs) -> {ok, Attrs} end
    ).


%%--------------------------------------------------------------------
%% @doc
%% Gets file details (see file_details.hrl) for each directory children
%% starting with Offset-th from specified StartId entry and up to Limit
%% of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_details(session:id(), lfm:file_key(), file_listing:options()) ->
    {ok, [lfm_attrs:file_details()], file_listing:pagination_token()} | lfm:error_reply().
get_children_details(SessId, FileKey, ListingOpts) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children_details{
            listing_options = ListingOpts
        },
        fun(#file_children_details{
            child_details = ChildrenInfo,
            pagination_token = ListingToken
        }) ->
            {ok, ChildrenInfo, ListingToken}
        end).


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
    recursive_file_listing:options()
) ->
    {ok, [recursive_file_listing:entry()], [file_meta:path()], recursive_file_listing:pagination_token()}.
get_files_recursively(SessId, FileKey, Options) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, resolve_symlink),
    
    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_recursive_file_list{
            listing_options = Options
        },
        fun(#recursive_file_list{
            entries = Result,
            inaccessible_paths = InaccessiblePaths,
            pagination_token = PaginationToken
        }) ->
            {ok, Result, InaccessiblePaths, PaginationToken}
        end).


-spec browse_dir_stats(session:id(), lfm:file_key(), oneprovider:id(), ts_browse_request:record()) ->
    {ok, ts_browse_result:record()} | {error, term()}.
browse_dir_stats(SessId, FileKey, ProviderId, BrowseRequest) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    Req = #provider_request{
        context_guid = Guid,
        provider_request = #browse_dir_stats{request = BrowseRequest}
    },

    Res = case oneprovider:is_self(ProviderId) of
        true ->
            worker_proxy:call(
                {id, fslogic_worker, file_id:guid_to_uuid(Guid)},
                {provider_request, SessId, Req}
            );
        false ->
            case connection:is_provider_connected(ProviderId) of
                true ->
                    % Provider is always allowed to read dir statistics of other providers.
                    %% @TODO VFS-9435 - let fslogic_worker handle routing between providers
                    {ok, fslogic_remote:route(user_ctx:new(?ROOT_SESS_ID), ProviderId, Req)};
                false ->
                    {error, ?EAGAIN}
            end
    end,
    
    case Res of
        {ok, #provider_response{status = #status{code = ?OK}, provider_response = #dir_stats_result{result = Result}}} ->
            {ok, Result};
        {ok, #provider_response{status = #status{code = Error}}} ->
            {error, Error};
        {error, _} = Error ->
            Error
    end.


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
    case get_children(SessId, ?FILE_REF(FileGuid), ListOpts) of
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
