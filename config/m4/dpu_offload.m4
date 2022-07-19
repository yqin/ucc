# -*- shell-script -*-
#
# Copyright 2022 NVIDIA CORPORATIONS. All rights reserved.
#
# See COPYING in top-level directory.
#
# Additional copyrights may follow
#
# $HEADER$
#

AC_MSG_CHECKING([DPU offloading support])
AC_ARG_WITH(dpu-offload,
            [AS_HELP_STRING([--with-dpu-offload=PATH],
             [Absolute path to the install directory for the DPU offloading libraries])],
            [DPU_OFFLOAD_INSTALL_DIR="${withval}"],
            [DPU_OFFLOAD_INSTALL_DIR=""])

DPU_OFFLOAD_INC=""
DPU_OFFLOAD_LIB=""
if test "x$DPU_OFFLOAD_INSTALL_DIR" = "x" ; then
    AC_MSG_RESULT([no])
else
    AC_MSG_RESULT([yes])
    DPU_OFFLOAD_INC="-I$DPU_OFFLOAD_INSTALL_DIR/include"
    DPU_OFFLOAD_LIB="-L$DPU_OFFLOAD_INSTALL_DIR/lib"
fi

CPPFLAGS_save="$CPPFLAGS"
LIBS_save="$LIBS"
LDFLAGS_save="$LDFLAGS"
AS_IF([test "x$DPU_OFFLOAD_LIB" != x],
      [
	AC_SUBST([CPPFLAGS], ["$CPPFLAGS $DPU_OFFLOAD_INC"])
	AC_SUBST([LDFLAGS], ["$LDFLAGS $DPU_OFFLOAD_LIB"])
      ],
    [])

dpu_offload_happy="yes"
AC_CHECK_LIB([dpuoffloaddaemon],
             [offload_engine_init],
             [],
             [
                AC_MSG_RESULT([Cannot use DPU offloading libraries])
                dpu_offload_happy="no"
             ]
             )
AC_CHECK_HEADERS([dpu_offload_envvars.h],
                 [],
                 [
                    AC_MSG_RESULT([Cannot find DPU offloading headers])
                    dpu_offload_happy="no"
                 ])

AS_IF([test "x$dpu_offload_happy" != xno],
    [
        AC_SUBST([CPPFLAGS],["$CPPFLAGS_save -DHAVE_DPU_OFFLOAD $DPU_OFFLOAD_INC"])
        AC_SUBST([LIBS],["$LIBS_save $DPU_OFFLOAD_LIB -ldpuoffloaddaemon"])
        echo DPU offload: enabled
    ],
    [
	AC_SUBST([CPPFLAGS], ["${CPPFLAGS_save}"])
	AC_SUBST([LDFLAGS], ["${LDFLAGS_save}"])
	echo DPU offload: disabled
    ])
