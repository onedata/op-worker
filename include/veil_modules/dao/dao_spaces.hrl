%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @todo: write me !
%% @end
%% ===================================================================
-author("Rafal Slota").

-ifndef(DAO_SPACES_HRL).
-define(DAO_SPACES_HRL, 1).

-define(file_space_info_extestion, space_info).
-record(space_info, {space_id = "", name = "", providers = []}).

-endif.