%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common defines for fslogic.
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-ifndef(FSLOGIC_COMMON_HRL).
-define(FSLOGIC_COMMON_HRL, 1).

-include("modules/fslogic/fslogic_mock.hrl").


-define(NOT_IMPLEMENTED, erlang:error(not_yet_implemented)).

-define(SPACES_BASE_DIR_NAME, <<"spaces">>).
-define(DIRECTORY_SEPARATOR, <<"/">>).


-record(fslogic_ctx, {session}).

-endif.