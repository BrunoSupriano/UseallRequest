=SEERRO(
   SE(
      MÍNIMO(MENOR(I2:R2;1)+730; O2+365) < HOJE();
      "VENCIDO";
      SE(
         MÍNIMO(MENOR(I2:R2;1)+730; O2+365) - HOJE() < 30;
         "VENCE EM " & (MÍNIMO(MENOR(I2:R2;1)+730; O2+365) - HOJE()) & " DIAS";
         "VENCE EM " & ARREDONDAR.PARA.BAIXO((MÍNIMO(MENOR(I2:R2;1)+730; O2+365) - HOJE())/30;0) & " MESES"
      )
   );
"")
