%% Copyright
-module(local_slave).
-author("michal").

%% API
-export([start/1, start/2, start/3, stop/1, stop/2]).

start(Node) ->
  ct_slave:start(Node).

start(Host, Node) ->
  ct_slave:start(Host, Node).

start(Host, Node, Options) ->
  ct_slave:start(Host, Node, Options).

stop(Node) ->
  ct_slave:stop(Node).

stop(Host, Node) ->
  ct_slave:stop(Host, Node).