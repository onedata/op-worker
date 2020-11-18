%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Function for converting paths to guids.
%%% @end
%%%--------------------------------------------------------------------
-module(guid_utils).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([ensure_guid/2]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Converts given file entry to FileGuid.
%% @end
%%--------------------------------------------------------------------
-spec ensure_guid(session:id(), fslogic_worker:file_guid_or_path()) ->
    {guid, fslogic_worker:file_guid()} | {error, term()}.
ensure_guid(_, {guid, FileGuid}) ->
    {guid, FileGuid};
ensure_guid(SessionId, {path, Path}) ->
    remote_utils:call_fslogic(SessionId, fuse_request,
        #resolve_guid_by_canonical_path{path = ensure_canonical_path(SessionId, Path)},
        fun(#guid{guid = Guid}) ->
            {guid, Guid}
        end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_canonical_path(session:id(), file_meta:path()) -> file_meta:path() | no_return().
ensure_canonical_path(SessionId, Path) ->
    case fslogic_path:split_skipping_dots(Path) of
        {ok, [<<"/">>]} ->
            <<"/">>;
        {ok, [<<"/">>, SpaceIdOrName | Rest]} ->
            SpaceId = case space_logic:get(SessionId, SpaceIdOrName) of
                {ok, _} ->
                    % Space record was fetched meaning this is space id
                    % and user belongs to this space
                    SpaceIdOrName;
                {error, _} ->
                    % Fetching space record failed meaning that this is space name and not id
                    % or user doesn't belong to it. Either way it is possible that user may have
                    % space of such name - this needs to be checked.
                    {ok, UserId} = session:get_user_id(SessionId),
                    case user_logic:get_space_by_name(SessionId, UserId, SpaceIdOrName) of
                        false ->
                            throw({error, ?ENOENT});
                        {true, Id} ->
                            Id
                    end
            end,
            middleware_utils:assert_space_supported_locally(SpaceId),
            filename:join([<<"/">>, SpaceId | Rest]);
        {error, _} ->
            throw({error, ?ENOENT})
    end.
