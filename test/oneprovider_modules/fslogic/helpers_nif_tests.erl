%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of helpers_nif.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(helpers_nif_tests).

-include_lib("eunit/include/eunit.hrl").


setup() ->
    helpers_nif:start("../").

teardown(_) ->
    ok.


%% This test checks if every single helpers_nif method has its C NIF implementation loaded.
%% If NIF library isn't loaded or if some implementations are missing, erlang wrappers return {error, 'NIF_not_loaded'}.
helpers_test_() ->
    {foreach, fun setup/0, fun teardown/1, 
        [fun getattr/0, fun access/0, fun mknod/0, fun unlink/0, fun rename/0, fun chmod/0, 
         fun chown/0, fun truncate/0, fun open/0, fun read/0, fun write/0, fun statfs/0, fun release/0, fun fsync/0
        ]}.

getattr() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:getattr(0, 0, "test", ["arg"], "path")).

access() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:access(0, 0, "test", ["arg"], "path", 0)).

mknod() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:mknod(0, 0, "test", ["arg"], "path", 0, 0)).

unlink() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:unlink(0, 0, "test", ["arg"], "path")).

rename() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:rename(0, 0, "test", ["arg"], "path", "path2")).

chmod() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:chmod(0, 0, "test", ["arg"], "path", 0)).

chown() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:chown(0, 0, "test", ["arg"], "path", 0, 0)).

truncate() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:truncate(0, 0, "test", ["arg"], "path", 0)).

open() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:open(0, 0, "test", ["arg"], "path", none)).

read() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:read(0, 0, "test", ["arg"], "path", 0, 0, none)).

write() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:write(0, 0, "test", ["arg"], "path", <<"buff">>, 0, none)).

statfs() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:statfs(0, 0, "test", ["arg"], "path")).

release() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:release(0, 0, "test", ["arg"], "path", none)).

fsync() -> 
    ?assertNot({error, 'NIF_not_loaded'} == helpers_nif:fsync(0, 0, "test", ["arg"], "path", 0, none)).

