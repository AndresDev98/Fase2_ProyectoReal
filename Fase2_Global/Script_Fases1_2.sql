
--E-- Actualiza campo Facturado del pendiente a 0000-00-00
UPDATE IN_PENDIENTES_FACTURAR SET Facturado = '0000-00-00'

--E-- Actualiza campo Facturado del pendiente cruzando por IN_FECHA_ULTIMA_FACTURA 
UPDATE IN_PENDIENTES_FACTURAR
SET
	IN_PENDIENTES_FACTURAR.Facturado = convert(VARCHAR(10), IN_FECHA_ULTIMA_FACTURA.F_Hasta, 121)
FROM IN_PENDIENTES_FACTURAR
	INNER JOIN IN_FECHA_ULTIMA_FACTURA ON IN_PENDIENTES_FACTURAR.num_susc =  IN_FECHA_ULTIMA_FACTURA.Num_Susc

--E-- Actualiza campo dif_num_dia del pendiente a 0
UPDATE IN_PENDIENTES_FACTURAR SET dif_num_dia = 0

--E-- Calcula diferencia dias cruzando por IN_FECHA_ULTIMA_FACTURA de las suscripciones <> 0000-00-00
UPDATE IN_PENDIENTES_FACTURAR
SET
	IN_PENDIENTES_FACTURAR.dif_num_dia = datediff(day,IN_PENDIENTES_FACTURAR.Facturado,eomonth(Getdate(), -1))
FROM IN_PENDIENTES_FACTURAR
	INNER JOIN IN_FECHA_ULTIMA_FACTURA ON IN_PENDIENTES_FACTURAR.num_susc =  IN_FECHA_ULTIMA_FACTURA.Num_Susc
WHERE IN_PENDIENTES_FACTURAR.Facturado <> '0000-00-00'

--E-- Calcula diferencia dias de las suscripciones = 0000-00-00
UPDATE IN_PENDIENTES_FACTURAR
SET dif_num_dia =  datediff(day,F_activacion_susc,eomonth(Getdate(), -1))
WHERE Facturado = '0000-00-00'

--E-- Actualiza el precio a 0 del campo Amount
UPDATE IN_PENDIENTES_FACTURAR  SET  Amount = 0

--E-- Actualiza el campo Amount cruzando por IN_MAPEO_PRODUCTOS_PRECIOS
UPDATE IPF
SET
	IPF.Amount = IPF.dif_num_dia * IMP.Precio_Diario 
FROM IN_PENDIENTES_FACTURAR AS IPF
	INNER JOIN IN_MAPEO_PRODUCTOS_PRECIOS AS IMP ON IMP.Producto = IPF.Producto

--E-- Elimina suscripciones del pendiente si el campo Facturado es <> 0000-00-00 y unico = 1
DELETE 
FROM IN_PENDIENTES_FACTURAR 
WHERE 
    Facturado <> '0000-00-00'
    AND unico = 1

--E-- Elimina suscripciones del pendiente si el campo Facturado es > al último día del mes y recurr = 1
DELETE 
FROM IN_PENDIENTES_FACTURAR 
WHERE   
    recurr = 1
    AND Facturado <> '0000-00-00'
    AND convert(DATETIME, Facturado, 102) > convert(DATETIME, eomonth(getdate(), -1), 102)

--E-- Elimina suscripciones del pendiente si el campo Facturado = al del análisis y recurr = 1
DELETE 
FROM IN_PENDIENTES_FACTURAR 
WHERE   
    recurr = 1
    AND Facturado <> '0000-00-00'
    AND convert(DATETIME, Facturado, 102) = convert(DATETIME, eomonth(getdate(), -1), 102)

--E-- Actualiza el campo Motivo de todas las suscripciones del pendiente a 'SIN MOTIVO'
UPDATE IN_PENDIENTES_FACTURAR
SET Motivo = 'SIN MOTIVO'

--E-- Calcula Motivo PREFACTURA GENERADA
UPDATE a
SET 
    a.Motivo = 'PREFACTURA GENERADA'
FROM IN_PENDIENTES_FACTURAR a
    INNER JOIN (SELECT SubscriptionName 
            FROM IN_DRAFTS 
            GROUP BY SubscriptionName ) t ON a.num_susc = t.SubscriptionName

--E-- Calcula Motivo CUENTAS BLOQUEADAS EN LOTE DE FACTURACIÓN CORRECTO
UPDATE a
SET 
    a.Motivo = 'CUENTAS BLOQUEADAS EN LOTE DE FACTURACIÓN CORRECTO'
FROM IN_PENDIENTES_FACTURAR a
    INNER JOIN (
        SELECT SubscriptionName
        FROM IN_SUBS_BLOQUEADAS
        GROUP BY SubscriptionName) t ON a.num_susc = t.SubscriptionName
WHERE 
    a.Motivo = 'SIN MOTIVO' AND 
    a.Batch ='Batch7';

--E-- Calcula Motivo CUENTAS BLOQUEADAS EN LOTE DE FACTURACIÓN INCORRECTO
UPDATE a 
SET
    a.Motivo = 'CUENTAS BLOQUEADAS EN LOTE DE FACTURACIÓN INCORRECTO'
FROM IN_PENDIENTES_FACTURAR a
    INNER JOIN (
        SELECT SubscriptionName
        FROM IN_SUBS_BLOQUEADAS
        GROUP BY SubscriptionName) t ON a.num_susc = t.SubscriptionName
WHERE 
    a.Motivo = 'SIN MOTIVO' AND 
    a.Batch <> 'Batch7';

--E-- Calcula Motivo CUENTAS NO BLOQUEADAS EN LOTE DE FACTURACIÓN DE BLOQUEO
UPDATE a
SET 
    a.Motivo = 'CUENTAS NO BLOQUEADAS EN LOTE DE FACTURACIÓN DE BLOQUEO'
FROM IN_PENDIENTES_FACTURAR a
LEFT JOIN (
        SELECT SubscriptionName
        FROM IN_SUBS_BLOQUEADAS
        GROUP BY SubscriptionName) t ON a.num_susc = t.SubscriptionName
WHERE 
	t.SubscriptionName IS NULL AND
    a.Motivo = 'SIN MOTIVO' AND 
    a.Batch = 'Batch7';

--E-- Calcula Motivo AUTO RENEW
UPDATE a 
SET
    a.Motivo = 'AUTO RENEW'
FROM IN_PENDIENTES_FACTURAR a
WHERE 
	a.Motivo = 'SIN MOTIVO' AND 
    UPPER(a.Autorenew ) = 'FALSE'

--E-- Calcula Motivo ALTA NUEVA
UPDATE IN_PENDIENTES_FACTURAR
SET
    Motivo = 'ALTA NUEVA'
WHERE 
	((MONTH(F_creacion_susc) = MONTH(eomonth(getdate(), -1)) AND YEAR(F_creacion_susc) = YEAR(getdate())	
	) OR 
	(
		convert(DATETIME, format(F_creacion_susc, 'yyyyMMdd')) >= convert(DATETIME, eomonth(getdate(), -2)) -1
	AND convert(DATETIME, format(F_creacion_susc, 'yyyyMMdd')) <= convert(DATETIME, eomonth(getdate(), -2)) 
	))	
    AND Motivo = 'SIN MOTIVO' 


--E-- Calcula Motivo CUENTA CON SUSCRIPCIÓN TRANSFERIDA
UPDATE a
SET 
    a.Motivo = 'CUENTA CON SUSCRIPCIÓN TRANSFERIDA'
FROM IN_PENDIENTES_FACTURAR a
    INNER JOIN (
        SELECT SubscriptionName
        FROM IN_OWNER_TRANSFET
        GROUP BY SubscriptionName) t ON a.num_susc = t.SubscriptionName
WHERE 
    Motivo = 'SIN MOTIVO' 

--E-- Calcula Motivo INCIDENCIA EN CAMBIO DE LOTE DE FACTURACIÓN(Batch 1)
UPDATE IN_PENDIENTES_FACTURAR
SET 
    Motivo = 'INCIDENCIA EN CAMBIO DE LOTE DE FACTURACIÓN(Batch 1)'
WHERE
    Motivo = 'SIN MOTIVO' 
	AND Batch = 'Batch1' 
	AND Motivo <> 'ALTA NUEVA'

--E-- Calcula Motivo PERIOCIDAD ANUAL
UPDATE IN_PENDIENTES_FACTURAR
SET
    Motivo = 'PERIOCIDAD ANUAL'
WHERE 
    Motivo = 'SIN MOTIVO' 
	AND Producto like '%Annual%'
