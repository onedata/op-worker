%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(model_behaviour).
-author("Rafal Slota").

-type model_action() :: save | get | delete | update | create | list.
-type model_type() :: atom().

%% API
-export([]).

%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback save() -> ok | {error, Error :: any()}.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback update() -> ok | {error, Error :: any()}.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback create() -> ok | {error, Error :: any()}.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback get() -> ok | {error, Error :: any()}.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback delete() -> ok | {error, Error :: any()}.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback exists() -> true | false.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback list() -> ok | {error, Error :: any()}.



%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback model_init() -> {RecordName :: atom(), RecordFields :: [atom()], [{model_type(), model_action()}]}.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback 'after'() -> ok.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback before() -> ok | {error, Error :: any()}.


%% %%--------------------------------------------------------------------
%% %% @doc
%% %% @todo: Write me!
%% %% @end
%% %%--------------------------------------------------------------------
%% -callback subscribe() -> ok.
%%
%%
%% %%--------------------------------------------------------------------
%% %% @doc
%% %% @todo: Write me!
%% %% @end
%% %%--------------------------------------------------------------------
%% -callback unsubscribe() -> ok | {error, Error :: any()}.