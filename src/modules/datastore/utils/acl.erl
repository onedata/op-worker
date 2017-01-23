%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for access control list management.
%%% @end
%%%--------------------------------------------------------------------
-module(acl).
-author("Tomasz Lichon").

-include("modules/fslogic/metadata.hrl").

-type acl() :: [#access_control_entity{}].

-export_type([acl/0]).

%% API
-export([get/1, exists/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file acl, or undefined if the acl is not defined.
%% @end
%%--------------------------------------------------------------------
-spec get(file_meta:uuid()) -> [#access_control_entity{}].
get(FileUuid) ->
    case xattr:get_by_name(FileUuid, ?ACL_KEY) of
        {ok, Val} ->
            fslogic_acl:from_json_format_to_acl(Val);
        {error, {not_found, custom_metadata}} ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if acl with given UUID exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(file_meta:uuid()) -> boolean().
exists(FileUuid) ->
    xattr:exists_by_name(FileUuid, ?ACL_XATTR_NAME).