%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of veilhelpers_nif.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(veilhelpers_nif_tests).

-include_lib("eunit/include/eunit.hrl").


setup() ->
    veilhelpers_nif:start("../").

teardown(_) ->
    ok.


%% This test checks if every single veilhelpers_nif method has its C NIF implementation loaded.
%% If NIF library isn't loaded or if some implementations are missing, erlang wrappers return {error, 'NIF_not_loaded'}.
veilhelpers_test_() ->
    {foreach, fun setup/0, fun teardown/1, 
        [fun getattr/0, fun access/0, fun mknod/0, fun unlink/0, fun rename/0, fun chmod/0, 
         fun chown/0, fun truncate/0, fun open/0, fun read/0, fun write/0, fun statfs/0, fun release/0, fun fsync/0
        ]}.

getattr() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:getattr("root", "root", "test", ["arg"], "path")).

access() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:access("root", "root", "test", ["arg"], "path", 0)).

mknod() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:mknod("root", "root", "test", ["arg"], "path", 0, 0)).

unlink() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:unlink("root", "root", "test", ["arg"], "path")).

rename() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:rename("root", "root", "test", ["arg"], "path", "path2")).

chmod() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:chmod("root", "root", "test", ["arg"], "path", 0)).

chown() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:chown("root", "root", "test", ["arg"], "path", 0, 0)).

truncate() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:truncate("root", "root", "test", ["arg"], "path", 0)).

open() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:open("root", "root", "test", ["arg"], "path", none)).

read() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:read("root", "root", "test", ["arg"], "path", 0, 0, none)).

write() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:write("root", "root", "test", ["arg"], "path", <<"buff">>, 0, none)).

statfs() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:statfs("root", "root", "test", ["arg"], "path")).

release() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:release("root", "root", "test", ["arg"], "path", none)).

fsync() -> 
    ?assertNot({error, 'NIF_not_loaded'} == veilhelpers_nif:fsync("root", "root", "test", ["arg"], "path", 0, none)).

