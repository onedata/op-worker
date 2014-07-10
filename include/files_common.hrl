%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: files_common header
%% @end
%% ===================================================================

-ifndef(FILES_COMMON_HRL).
-define(FILES_COMMON_HRL, 1).

-define(PATH_SEPARATOR, $/).

%% Used in #file{type = ?}
-define(REG_TYPE, 0).
-define(DIR_TYPE, 1).
-define(LNK_TYPE, 2).

%% File permissions defines

%% Sticky bit
-define(STICKY_BIT, 8#1000).

%% User
-define(RD_USR_PERM, 8#400).
-define(WR_USR_PERM, 8#200).
-define(EX_USR_PERM, 8#100).
-define(RW_USR_PERM, ?RD_USR_PERM bor ?WR_USR_PERM).
-define(RWE_USR_PERM, ?RD_USR_PERM bor ?WR_USR_PERM bor ?EX_USR_PERM).

%% Group
-define(RD_GRP_PERM, 8#40).
-define(WR_GRP_PERM, 8#20).
-define(EX_GRP_PERM, 8#10).
-define(RW_GRP_PERM, ?RD_GRP_PERM bor ?WR_GRP_PERM).
-define(RWE_GRP_PERM, ?RD_GRP_PERM bor ?WR_GRP_PERM bor ?EX_GRP_PERM).

%% Others
-define(RD_OTH_PERM, 8#4).
-define(WR_OTH_PERM, 8#2).
-define(EX_OTH_PERM, 8#1).
-define(RW_OTH_PERM, ?RD_OTH_PERM bor ?WR_OTH_PERM).
-define(RWE_OTH_PERM, ?RD_OTH_PERM bor ?WR_OTH_PERM bor ?EX_OTH_PERM).

%% All
-define(RD_ALL_PERM, ?RD_USR_PERM bor ?RD_GRP_PERM bor ?RD_OTH_PERM).
-define(WR_ALL_PERM, ?WR_USR_PERM bor ?WR_GRP_PERM bor ?WR_OTH_PERM).
-define(EX_ALL_PERM, ?EX_USR_PERM bor ?EX_GRP_PERM bor ?EX_OTH_PERM).

-endif. %% FILES_COMMON_HRL