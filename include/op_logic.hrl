%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions concerning entity logic.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ENTITY_LOGIC_HRL).
-define(ENTITY_LOGIC_HRL, 1).

-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

% Record expressing entity logic request client (REST and Graph Sync).
-record(client, {
    % root is allowed to do anything, it must be used with caution
    % (should not be used in any kind of external API!)
    type = nobody :: user | provider | root | nobody,
    id = <<"">> :: binary()
}).

% Record expressing entity logic request
-record(el_req, {
    client = #client{} :: op_logic:client(),
    gri :: op_logic:gri(),
    operation = create :: op_logic:operation(),
    data = #{} :: op_logic:data(),
    auth_hint = undefined :: undefined | op_logic:auth_hint()
}).

% Convenience macros for concise code
-define(USER, #client{type = user}).
-define(USER(__Id), #client{type = user, id = __Id}).
-define(PROVIDER, #client{type = provider}).
-define(PROVIDER(__Id), #client{type = provider, id = __Id}).
-define(NOBODY, #client{type = nobody}).
-define(ROOT, #client{type = root}).

% Regexp to validate names. Name must be 2-50 characters long and composed of
% UTF-8 letters, digits, brackets and underscores.
% Dashes, spaces and dots are allowed (but not at the beginning or the end).
-define(NAME_FIRST_CHARS_ALLOWED, <<")(\\w_">>).
-define(NAME_MIDDLE_CHARS_ALLOWED, <<">>)(\\w_ .-">>).
-define(NAME_LAST_CHARS_ALLOWED, ?NAME_FIRST_CHARS_ALLOWED).
-define(NAME_MAXIMUM_LENGTH, 50).

-endif.
