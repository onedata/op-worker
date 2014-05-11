%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Write me !
%% @end
%% ===================================================================
-module(fslogic_context).
-author("Rafal Slota").

-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao.hrl").

%% API
-export([get_fuse_id/0, set_fuse_id/1, get_user_dn/0, set_user_dn/1, set_protocol_version/1, get_protocol_version/0, get_user_id/0]).

%% ====================================================================
%% API functions
%% ====================================================================

get_user_dn() ->
    get(user_dn).

set_user_dn(UserDN) ->
    put(user_dn, UserDN).

get_fuse_id() ->
    get(fuse_id).

set_fuse_id(FuseID) ->
    put(fuse_id, FuseID).

set_protocol_version(PVers) ->
    put(protocol_version, PVers).

get_protocol_version() ->
    get(protocol_version).

%% get_user_id/0
%% ====================================================================
%% @doc Gets user's id.
%% @end
-spec get_user_id() -> Result when
    Result :: {ok, UserID} | {error, ErrorDesc},
    UserID :: term(),
    ErrorDesc :: atom.
%% ====================================================================
get_user_id() ->
    UserDN = get_user_dn(),
    case UserDN of
        undefined -> {ok, ?CLUSTER_USER_ID};
        DN ->
            case fslogic_objects:get_user({dn, DN}) of
                {ok, #veil_document{uuid = UID}} -> {ok, UID};
                _ -> {error, get_user_error}
            end
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
