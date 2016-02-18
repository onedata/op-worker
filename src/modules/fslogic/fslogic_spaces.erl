%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_spaces).
-author("Krzysztof Trzepla").
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

%% API
-export([get_default_space/1, get_default_space_id/1, get_space/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns default space document.
%% @end
%%--------------------------------------------------------------------
-spec get_default_space(UserIdOrCTX :: fslogic_worker:ctx() | onedata_user:id()) ->
    {ok, datastore:document()} | datastore:get_error().
get_default_space(UserIdOrCTX) ->
    {ok, DefaultSpaceId} = get_default_space_id(UserIdOrCTX),
    file_meta:get_space_dir(DefaultSpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns default space ID.
%% @end
%%--------------------------------------------------------------------
-spec get_default_space_id(UserIdOrCTX :: fslogic_worker:ctx() | onedata_user:id()) ->
    {ok, SpaceId :: binary()}.
get_default_space_id(CTX = #fslogic_ctx{}) ->
    UserId = fslogic_context:get_user_id(CTX),
    get_default_space_id(UserId);
get_default_space_id(UserId) ->
    {ok, #document{value = #onedata_user{space_ids = [DefaultSpaceId | _]}}} =
        onedata_user:get(UserId),
    {ok, DefaultSpaceId}.

%%--------------------------------------------------------------------
%% @doc
%% Returns space document for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_space(FileEntry :: fslogic_worker:file(), UserId :: onedata_user:id()) ->
    {ok, ScopeDoc :: datastore:document()} | {error, Reason :: term()}.
get_space(FileEntry, UserId) ->
    DefaultSpaceUUID = fslogic_uuid:default_space_uuid(UserId),
    SpacesDir = fslogic_uuid:spaces_uuid(UserId),
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),

    SpaceId = case FileUUID of
        <<"">> ->
            throw({not_a_space, FileEntry});
        SpacesDir ->
            throw({not_a_space, FileEntry});
        DefaultSpaceUUID ->
            {ok, DefaultSpaceId} = get_default_space_id(UserId),
            DefaultSpaceId;
        _ ->
            ?info("Decoding UUID ~p ~p", [FileUUID, FileEntry]),
            BinFileUUID = base64:decode(FileUUID),
            case binary_to_term(BinFileUUID) of
                {space, SpaceId0} ->
                    SpaceId0;
                {{space_id, SpaceId0}, _FileUUID} ->
                    SpaceId0
            end
    end,

    {ok, SpaceIds} = onedata_user:get_spaces(UserId),
    case (is_list(SpaceIds) andalso lists:member(SpaceId, SpaceIds)) orelse UserId == ?ROOT_USER_ID of
        true ->
            {ok, SpaceDoc} = file_meta:get(fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)),
            {ok, SpaceDoc};
        false -> throw({not_a_space, FileEntry})
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================