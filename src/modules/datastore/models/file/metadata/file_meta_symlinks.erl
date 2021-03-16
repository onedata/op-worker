%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of file_meta symlinks.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_symlinks).
-author("Michal Wrzeszcz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([new_symlink_doc/5, get_symlink/1]).

-type symlink() :: file_meta:path().
-export_type([symlink/0]).

% POSIX defines that symlinks have 777 permission by default
-define(NEW_DOC_MODE, 8#777).

%%%===================================================================
%%% API
%%%===================================================================

-spec new_symlink_doc(file_meta:name(), file_meta:uuid(), od_space:id(), od_user:id(), symlink()) -> file_meta:doc().
new_symlink_doc(Name, ParentUuid, SpaceId, Owner, Link) ->
    #document{
        key = undefined,
        value = #file_meta{
            name = Name,
            type = ?SYMLINK_TYPE,
            mode = ?NEW_DOC_MODE,
            owner = Owner,
            parent_uuid = ParentUuid,
            provider_id = oneprovider:get_id(),
            symlink = Link
        },
        scope = SpaceId
    }.

-spec get_symlink(file_meta:doc() | file_meta:uuid()) -> {ok, symlink()} | {error, term()}.
get_symlink(#document{value = #file_meta{type = ?SYMLINK_TYPE, symlink = Link}}) ->
    {ok, Link};
get_symlink(#document{value = #file_meta{}}) ->
    {error, ?EINVAL};
get_symlink(Key) ->
    case file_meta:get(Key) of
        {ok, Doc} ->
            get_symlink(Doc);
        Other -> Other
    end.