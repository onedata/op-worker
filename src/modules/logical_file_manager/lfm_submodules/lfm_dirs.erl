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
    get_children/3,
    get_children_attrs/3,
    get_child_attr/3,
    get_children_details/3,
    get_children_count/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec mkdir(session:id(), file_meta:path()) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
mkdir(SessId, Path) ->
    mkdir(SessId, Path, undefined).


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
    {ok, Mode} = application:get_env(?APP_NAME, default_dir_mode),
    mkdir(SessId, ParentGuid, Name, Mode);
mkdir(SessId, ParentGuid0, Name, Mode) ->
    {ok, ParentGuid1} = guid_utils:resolve_if_symlink(SessId, ParentGuid0),

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
-spec get_children(session:id(), lfm:file_key(), file_meta:list_opts()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}], file_meta:list_extended_info()} | lfm:error_reply().
get_children(SessId, FileKey, ListOpts) ->
    FileGuid = guid_utils:resolve_file_key(SessId, FileKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children{
            offset = maps:get(offset, ListOpts, undefined),
            size = maps:get(size, ListOpts, undefined),
            index_startid = maps:get(last_name, ListOpts, undefined),
            index_token = maps:get(token, ListOpts, undefined)
        },
        fun(#file_children{
            child_links = List,
            is_last = IsLast,
            index_token = ReturnedToken
        }) ->
            Children = [{Guid, FileName} || #child_link{guid = Guid, name = FileName} <- List],
            ListExtendedInfo = #{is_last => IsLast},
            {ok, Children, maps_utils:put_if_defined(ListExtendedInfo, token, ReturnedToken)}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each directory children
%% starting with Offset-th entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(session:id(), lfm:file_key(), file_meta:list_opts()) ->
    {ok, [#file_attr{}], file_meta:list_extended_info()} | lfm:error_reply().
get_children_attrs(SessId, FileKey, ListOpts) ->
    FileGuid = guid_utils:resolve_file_key(SessId, FileKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children_attrs{
            offset = maps:get(offset, ListOpts, undefined),
            size = maps:get(size, ListOpts, undefined),
            index_token = maps:get(token, ListOpts, undefined),
            include_link_count = true
        },
        fun(#file_children_attrs{
            child_attrs = Attrs,
            is_last = IsLast,
            index_token = ReturnedToken
        }) ->
            ListExtendedInfo = #{is_last => IsLast},
            {ok, Attrs, maps_utils:put_if_defined(ListExtendedInfo, token, ReturnedToken)}
        end).



-spec get_child_attr(session:id(), file_id:file_guid(), file_meta:name()) ->
    {ok, #file_attr{}} | lfm:error_reply().
get_child_attr(SessId, ParentGuid0, ChildName)  ->
    {ok, ParentGuid1} = guid_utils:resolve_if_symlink(SessId, ParentGuid0),

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
-spec get_children_details(session:id(), lfm:file_key(), file_meta:list_opts()) ->
    {ok, [lfm_attrs:file_details()], file_meta:list_extended_info()} | lfm:error_reply().
get_children_details(SessId, FileKey, ListOpts) ->
    FileGuid = guid_utils:resolve_file_key(SessId, FileKey, resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children_details{
            offset = maps:get(offset, ListOpts, undefined),
            size = maps:get(size, ListOpts, undefined),
            index_startid = maps:get(last_name, ListOpts, undefined)
        },
        fun(#file_children_details{
            child_details = ChildrenInfo,
            is_last = IsLast
        }) ->
            {ok, ChildrenInfo, #{is_last => IsLast}}
        end).


-spec get_children_count(session:id(), lfm:file_key()) ->
    {ok, non_neg_integer()} | lfm:error_reply().
get_children_count(SessId, FileKey) ->
    FileGuid = guid_utils:resolve_file_key(SessId, FileKey, resolve_symlink),

    case count_children(SessId, FileGuid) of
        {ok, ChildrenNum} -> {ok, ChildrenNum};
        {error, Err} -> {error, Err}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec count_children(session:id(), fslogic_worker:file_guid()) ->
    {ok, non_neg_integer()} | lfm:error_reply().
count_children(SessId, FileGuid) ->
    {ok, BatchSize} = application:get_env(?APP_NAME, ls_batch_size),
    count_children(SessId, FileGuid, #{token => ?INITIAL_LS_TOKEN, size => BatchSize}, 0).


%% @private
-spec count_children( session:id(), fslogic_worker:file_guid(),
    file_meta:list_opts(), Acc :: non_neg_integer()) ->
    {ok, non_neg_integer()} | lfm:error_reply().
count_children(SessId, FileGuid, ListOpts, Acc) ->
    case get_children(SessId, {guid, FileGuid}, ListOpts) of
        {ok, List, ListExtendedInfo} ->
            case maps:get(is_last, ListExtendedInfo) of
                true ->
                    {ok, Acc + length(List)};
                false ->
                    ListOpts2 = ListOpts#{token => maps:get(token, ListExtendedInfo)},
                    count_children(SessId, FileGuid, ListOpts2, Acc + length(List))
            end;
        {error, _} = Error ->
            Error
    end.
