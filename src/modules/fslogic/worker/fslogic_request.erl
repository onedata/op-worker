%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for obtaining and modifying things related to any
%%% fslogic request.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_request).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_file_partial_ctx/2, get_target_providers/3]).

% Part of a file's content that a request serves, and hence must find on the
% local storage to be handled here - a block, or the whole file when the request
% carries no range of its own. Requests that serve no content have no read scope.
-type read_scope() :: fslogic_blocks:block() | whole_file.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get file_ctx record associated with request. If request does not point to
%% specific file, the function returns undefined.
%% @end
%%--------------------------------------------------------------------
-spec get_file_partial_ctx(user_ctx:ctx(), fslogic_worker:request()) ->
    file_partial_ctx:ctx() | undefined.
get_file_partial_ctx(UserCtx, #fuse_request{fuse_request = #resolve_guid{path = Path}}) ->
    file_partial_ctx:new_by_logical_path(UserCtx, Path);
get_file_partial_ctx(UserCtx, #fuse_request{fuse_request = #resolve_guid_by_canonical_path{path = Path}}) ->
    file_partial_ctx:new_by_canonical_path(UserCtx, Path);
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #resolve_guid_by_relative_path{root_file = RelRootGuid}}) ->
    file_partial_ctx:new_by_guid(RelRootGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #ensure_dir{root_file = RelRootGuid}}) ->
    file_partial_ctx:new_by_guid(RelRootGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #file_request{context_guid = FileGuid}}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #get_fs_stats{file_id = FileGuid}}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{}) ->
    undefined;
get_file_partial_ctx(_UserCtx, #provider_request{context_guid = FileGuid}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, #proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @doc
%% Get providers capable of handling given request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers(user_ctx:ctx(), file_partial_ctx:ctx() | undefined, fslogic_worker:request()) ->
    [oneprovider:id()].
get_target_providers(_UserCtx, undefined, _) ->
    [oneprovider:get_id()];
get_target_providers(UserCtx, File, #fuse_request{
    fuse_request = #resolve_guid{}
}) ->
    get_target_providers_for_attr_req(UserCtx, File);
get_target_providers(UserCtx, File, #fuse_request{
    fuse_request = #resolve_guid_by_relative_path{}
}) ->
    get_target_providers_for_attr_req(UserCtx, File);
get_target_providers(UserCtx, File, #fuse_request{
    fuse_request = #ensure_dir{}
}) ->
    get_target_providers_for_attr_req(UserCtx, File);
get_target_providers(UserCtx, File, #fuse_request{fuse_request = #file_request{
    file_request = #get_file_attr{}
}}) ->
    get_target_providers_for_attr_req(UserCtx, File);
get_target_providers(UserCtx, File, #fuse_request{fuse_request = #file_request{
    file_request = #release{handle_id = HandleId}
}}) ->
    get_target_providers_for_handle(UserCtx, File, HandleId);
get_target_providers(UserCtx, File, #fuse_request{fuse_request = #file_request{
    file_request = #fsync{handle_id = HandleId}
}}) when HandleId =/= undefined ->
    get_target_providers_for_handle(UserCtx, File, HandleId);
get_target_providers(UserCtx, File, Req) ->
    get_target_providers_for_file(UserCtx, File, infer_read_scope(Req)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get providers capable of handling resolve_guid/get_attr request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers_for_attr_req(user_ctx:ctx(), file_partial_ctx:ctx()) ->
    [oneprovider:id()].
get_target_providers_for_attr_req(UserCtx, FileCtx) ->
    case file_partial_ctx:is_space_dir_const(FileCtx) of
        true ->
            [oneprovider:get_id()];
        false ->
            get_target_providers_for_file(UserCtx, FileCtx)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get providers capable of handling a request bound to an already opened file
%% handle. A handle exists only on the provider that opened the file, so once
%% one of them has done that on this provider's behalf, everything carrying the
%% handle has to follow - regardless of where the content has meanwhile moved.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers_for_handle(
    user_ctx:ctx(), file_partial_ctx:ctx(), storage_driver:handle_id()
) ->
    [oneprovider:id()].
get_target_providers_for_handle(UserCtx, FilePartialCtx, HandleId) ->
    case session_remote_handles:get(user_ctx:get_session_id(UserCtx), HandleId) of
        {ok, ProviderId} ->
            [ProviderId];
        {error, _} ->
            % not a handle of a handed over open - hence one of this provider
            get_target_providers_for_file(UserCtx, FilePartialCtx)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv get_target_providers_for_file(UserCtx, FilePartialCtx, undefined)
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers_for_file(user_ctx:ctx(), file_partial_ctx:ctx()) ->
    [oneprovider:id()].
get_target_providers_for_file(UserCtx, FilePartialCtx) ->
    get_target_providers_for_file(UserCtx, FilePartialCtx, undefined).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get providers capable of handling generic request. A request serving file
%% content can only be handled here if the content is on the local storage -
%% a readonly storage is one that the content cannot be fetched onto, so such
%% a request is handed over to a provider able to serve it.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers_for_file(user_ctx:ctx(), file_partial_ctx:ctx(), read_scope() | undefined) ->
    [oneprovider:id()].
get_target_providers_for_file(UserCtx, FilePartialCtx, ReadScope) ->
    case file_partial_ctx:is_user_root_dir_const(FilePartialCtx, UserCtx) of
        true ->
            [oneprovider:get_id()];
        false ->
            SpaceId = file_partial_ctx:get_space_id_const(FilePartialCtx),
            SessionId = user_ctx:get_session_id(UserCtx),
            % Eventual lack of access to space (not a member, data access caveats, etc.)
            % badmatch with concrete error will be propagated and handled in fslogic_errors
            {ok, Providers} = space_logic:get_provider_ids(SessionId, SpaceId),
            LocalProviderId = oneprovider:get_id(),
            case lists:member(LocalProviderId, Providers) of
                true ->
                    case is_read_servable_locally(UserCtx, FilePartialCtx, SpaceId, ReadScope) of
                        true ->
                            [LocalProviderId];
                        false ->
                            get_read_serving_providers(
                                FilePartialCtx, SpaceId, ReadScope, Providers -- [LocalProviderId]
                            )
                    end;
                false ->
                    Providers
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the part of the file's content that the request serves, or undefined
%% if it serves none. A synchronization request with no block of its own
%% concerns the whole file, and so does opening it.
%% @end
%%--------------------------------------------------------------------
-spec infer_read_scope(fslogic_worker:request()) -> read_scope() | undefined.
infer_read_scope(#fuse_request{fuse_request = #file_request{file_request = #open_file{flag = read}}}) ->
    whole_file;
infer_read_scope(#fuse_request{fuse_request = #file_request{
    file_request = #open_file_with_extended_info{flag = read}
}}) ->
    whole_file;
infer_read_scope(#fuse_request{fuse_request = #file_request{file_request = #synchronize_block{block = Block}}}) ->
    utils:ensure_defined(Block, whole_file);
infer_read_scope(#fuse_request{fuse_request = #file_request{
    file_request = #synchronize_block_and_compute_checksum{block = Block}
}}) ->
    utils:ensure_defined(Block, whole_file);
infer_read_scope(#fuse_request{fuse_request = #file_request{
    file_request = #block_synchronization_request{block = Block}
}}) ->
    utils:ensure_defined(Block, whole_file);
infer_read_scope(#proxyio_request{proxyio_request = #remote_read{offset = Offset, size = Size}}) ->
    #file_block{offset = Offset, size = Size};
infer_read_scope(_Req) ->
    undefined.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tells whether the requested content can be read from the local storage as is.
%% Anything missing from a writable storage is simply fetched onto it, so only
%% a readonly one calls for looking into the file.
%% @end
%%--------------------------------------------------------------------
-spec is_read_servable_locally(
    user_ctx:ctx(), file_partial_ctx:ctx(), od_space:id(), read_scope() | undefined
) ->
    boolean().
is_read_servable_locally(_UserCtx, _FilePartialCtx, _SpaceId, undefined) ->
    true;
is_read_servable_locally(UserCtx, FilePartialCtx, SpaceId, ReadScope) ->
    case space_logic:has_readonly_support_from(SpaceId, oneprovider:get_id()) of
        false ->
            true;
        true ->
            % a request handed over by another provider is served here or not at all,
            % so that it can never be passed back and forth between the two
            case is_proxied_request(UserCtx) of
                true ->
                    true;
                false ->
                    {FileCtx, _SpaceId} = file_ctx:new_by_partial_context(FilePartialCtx),
                    holds_content(file_ctx:get_local_file_location_doc_const(FileCtx), ReadScope)
            end
    end.

%% @private
-spec is_proxied_request(user_ctx:ctx()) -> boolean().
is_proxied_request(UserCtx) ->
    case session:get_proxy_via(user_ctx:get_session_id(UserCtx)) of
        {ok, ProxyVia} -> ProxyVia =/= undefined;
        {error, _} -> false
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tells whether the given replica covers the requested content in its entirety.
%% NOTE: the location document must come from fslogic_location_cache, as the up
%% to date blocks may still be sitting in the memory of the file's synchronizer.
%% @end
%%--------------------------------------------------------------------
-spec holds_content(file_location:doc() | undefined, read_scope()) -> boolean().
holds_content(undefined, _ReadScope) ->
    % nothing of the file was ever put on this storage
    false;
holds_content(#document{value = #file_location{storage_file_created = false}}, _ReadScope) ->
    false;
holds_content(#document{value = #file_location{size = undefined}}, _ReadScope) ->
    % the extent of the replica is unknown, so it cannot be told to cover anything
    false;
holds_content(#document{value = #file_location{size = Size}} = LocationDoc, whole_file) ->
    holds_content(LocationDoc, #file_block{offset = 0, size = Size});
holds_content(#document{value = #file_location{size = Size}} = LocationDoc, #file_block{
    offset = Offset,
    size = ReadSize
}) ->
    % consolidation drops a block of no size, leaving nothing to be missing - which
    % is what both an empty file and a read reaching past the end of one amount to
    RequestedBlocks = fslogic_blocks:consolidate([#file_block{
        offset = Offset, size = min(ReadSize, max(0, Size - Offset))
    }]),
    [] == fslogic_blocks:invalidate(RequestedBlocks, fslogic_location_cache:get_blocks(LocationDoc)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the providers to hand the request over to, the ones that can serve it
%% outright coming first. A provider supporting the space with a readonly storage
%% is only a candidate if it holds the whole of the requested content, as lacking
%% any of it, it would have nowhere to fetch the rest onto.
%% @end
%%--------------------------------------------------------------------
-spec get_read_serving_providers(
    file_partial_ctx:ctx(), od_space:id(), read_scope(), [oneprovider:id()]
) ->
    [oneprovider:id()].
get_read_serving_providers(_FilePartialCtx, _SpaceId, _ReadScope, []) ->
    % nothing to hand the request over to - fslogic_worker answers with ?ENOTSUP
    [];
get_read_serving_providers(_FilePartialCtx, _SpaceId, _ReadScope, [_] = SoleRemoteProvider) ->
    % nothing to choose from; whether it can serve the request is for it to find out
    SoleRemoteProvider;
get_read_serving_providers(FilePartialCtx, SpaceId, ReadScope, RemoteProviders) ->
    {FileCtx, _SpaceId} = file_ctx:new_by_partial_context(FilePartialCtx),
    {LocationDocs, _FileCtx2} = file_ctx:get_file_location_docs(FileCtx),

    {ContentHolders, RemainingProviders} = lists:partition(fun(ProviderId) ->
        holds_content(find_location(ProviderId, LocationDocs), ReadScope)
    end, RemoteProviders),

    ContentHolders ++ lists:filter(fun(ProviderId) ->
        not space_logic:has_readonly_support_from(SpaceId, ProviderId)
    end, RemainingProviders).

%% @private
-spec find_location(oneprovider:id(), [file_location:doc()]) -> file_location:doc() | undefined.
find_location(ProviderId, LocationDocs) ->
    FoundLocations = [Doc || #document{value = #file_location{provider_id = Id}} = Doc <- LocationDocs,
        Id == ProviderId],
    case FoundLocations of
        [LocationDoc] -> LocationDoc;
        _ -> undefined
    end.
