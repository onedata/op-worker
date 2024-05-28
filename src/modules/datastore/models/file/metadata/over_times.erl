%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
% fixme
%%% @end
%%%-------------------------------------------------------------------
-module(over_times). % fixme name (sth with events??) % fixme move
-author("Michal Stanisz").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([create/3, update/2, get/1, delete/1]).

% fixme implement
%%%===================================================================
%%% API
%%%===================================================================

create(FileGuid, IgnoreInChanges, Times) ->
    case times:create2(file_id:guid_to_uuid(FileGuid), file_id:guid_to_space_id(FileGuid), IgnoreInChanges, Times) of
        {ok, _} ->
            fslogic_event_emitter:emit_sizeless_file_attrs_changed(file_ctx:new_by_guid(FileGuid));
        {error, _} = Error ->
            Error
    end.
    
update(FileGuid, NewTimes) ->
    case times:update2(file_id:guid_to_uuid(FileGuid), NewTimes) of
        ok ->
            fslogic_event_emitter:emit_sizeless_file_attrs_changed(file_ctx:new_by_guid(FileGuid));
        {error, no_change} ->
            ok
    end.

get(FileGuid) ->
    times:get2(file_id:guid_to_uuid(FileGuid)).

delete(FileGuid) ->
    times:delete2(file_id:guid_to_uuid(FileGuid)).