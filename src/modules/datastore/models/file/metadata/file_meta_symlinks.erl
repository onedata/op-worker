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
-export([new_doc/5, readlink/1]).

-type symlink() :: file_meta:path().
-export_type([symlink/0]).

% POSIX defines that symlinks have 777 permission by default
-define(MODE, 8#777).

%%%===================================================================
%%% API
%%%===================================================================

-spec new_doc(file_meta:name(), file_meta:uuid(), od_space:id(), od_user:id(), symlink()) -> file_meta:doc().
new_doc(Name, ParentUuid, SpaceId, Owner, Link) ->
    #document{
        key = fslogic_uuid:gen_symlink_uuid(),
        value = #file_meta{
            name = Name,
            type = ?SYMLINK_TYPE,
            mode = ?MODE,
            owner = Owner,
            parent_uuid = ParentUuid,
            provider_id = oneprovider:get_id(),
            symlink_value = Link
        },
        scope = SpaceId
    }.

-spec readlink(file_meta:doc() | file_meta:uuid()) -> {ok, symlink()} | {error, term()}.
readlink(#document{value = #file_meta{type = ?SYMLINK_TYPE, symlink_value = Link}}) ->
    {ok, Link};
readlink(#document{value = #file_meta{}}) ->
    {error, ?EINVAL};
readlink(Key) ->
    case file_meta:get(Key) of
        {ok, Doc} ->
            readlink(Doc);
        Other -> Other
    end.